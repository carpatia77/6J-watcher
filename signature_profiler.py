import duckdb
import numpy as np
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional
from config import Config

logger = logging.getLogger(__name__)

class SignatureProfiler:
    """
    Calcula MFE/MAE via DuckDB Window Functions e gera Tabelas de Percentis
    Empíricos para normalização não-paramétrica de Order Flow.
    """
    def __init__(self, db_path: str, cfg: Optional[Config] = None):
        self.cfg = cfg or Config()
        self.db_path = db_path
        self.conn = duckdb.connect(db_path, read_only=True)

    def define_session(self, hour: int) -> str:
        if 0 <= hour < 8: return "ASIAN"
        if 8 <= hour < 13: return "LONDON"
        if 13 <= hour < 22: return "NEW_YORK"
        return "OFF_HOURS"

    def build_profile(self, symbol: str, lookback_days: int = 30, horizon_minutes: int = 30, tick_size: Optional[float] = None) -> dict:
        """
        Executa o pipeline de perfilamento empírico.
        Toda a agregação, percentis e regras direcionais rodam nativamente no motor C++ do DuckDB.
        Zero risco de OOM na RAM do Python (O(resultado)).
        """
        if not isinstance(horizon_minutes, int) or not (1 <= horizon_minutes <= 1440):
            raise ValueError("horizon_minutes must be an int between 1 and 1440")
            
        tick_size = tick_size or self.cfg.tick_size
        interval_clause = f"INTERVAL '{horizon_minutes}' MINUTE"
        cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d %H:%M:%S')
        
        # 1. CÁLCULO VETORIAL DE MFE/MAE NO DUCKDB E WIN/LOSS
        base_query = f"""
        WITH cluster_excursions AS (
            SELECT 
                c.timestamp,
                c.behavior_signature,
                c.session,
                (c.total_bid + c.total_ask) AS total_vol,
                ABS(c.total_bid - c.total_ask) AS imbalance,
                c.price AS c_price,
                -- O GROUP BY garante 1 linha por cluster, agregando eventos na janela
                COALESCE(MAX(t.price), c.price) AS max_future_price,
                COALESCE(MIN(t.price), c.price) AS min_future_price
            FROM liquidity_clusters c
            LEFT JOIN tape_events t 
              ON c.symbol = t.symbol 
             AND t.timestamp > c.timestamp 
             AND t.timestamp <= c.timestamp + {interval_clause}
            WHERE c.symbol = '{symbol}' AND c.timestamp > '{cutoff}'
            GROUP BY c.timestamp, c.behavior_signature, c.session, c.total_bid, c.total_ask, c.price
        ),
        mfe_mae_calc AS (
            SELECT 
                *,
                CASE WHEN behavior_signature IN ('iceberg_accumulation', 'breakout_genuine', 'magnet_effect') THEN TRUE ELSE FALSE END AS is_bullish,
                CASE WHEN behavior_signature IN ('iceberg_accumulation', 'breakout_genuine', 'magnet_effect') 
                     THEN max_future_price - c_price 
                     ELSE c_price - min_future_price END AS mfe,
                CASE WHEN behavior_signature IN ('iceberg_accumulation', 'breakout_genuine', 'magnet_effect') 
                     THEN c_price - min_future_price 
                     ELSE max_future_price - c_price END AS mae
            FROM cluster_excursions
        )
        SELECT 
            *,
            CASE WHEN mfe > ABS(mae) AND mfe > 0 THEN 1 ELSE 0 END AS win,
            CASE WHEN mfe > 0 THEN mfe ELSE 0 END AS gross_profit,
            CASE WHEN mae < 0 THEN ABS(mae) ELSE 0 END AS gross_loss
        FROM mfe_mae_calc
        """
        
        try:
            base_rel = self.conn.sql(base_query)
            
            sig_query = """
                SELECT 
                    behavior_signature,
                    session,
                    COUNT(*) AS cluster_count,
                    SUM(win) AS wins,
                    SUM(gross_profit) AS total_gross_profit,
                    SUM(gross_loss) AS total_gross_loss,
                    AVG(mfe) AS avg_mfe
                FROM base_result
                GROUP BY behavior_signature, session
            """
            sig_df = base_rel.query("base_result", sig_query).fetchdf()
            
            perc_query = """
                SELECT 
                    session,
                    COUNT(*) AS session_count,
                    QUANTILE_CONT(total_vol, 0.50) AS vol_p50,
                    QUANTILE_CONT(total_vol, 0.75) AS vol_p75,
                    QUANTILE_CONT(total_vol, 0.90) AS vol_p90,
                    QUANTILE_CONT(total_vol, 0.95) AS vol_p95,
                    QUANTILE_CONT(total_vol, 0.99) AS vol_p99,
                    QUANTILE_CONT(imbalance, 0.50) AS imb_p50,
                    QUANTILE_CONT(imbalance, 0.75) AS imb_p75,
                    QUANTILE_CONT(imbalance, 0.90) AS imb_p90,
                    QUANTILE_CONT(imbalance, 0.95) AS imb_p95,
                    QUANTILE_CONT(imbalance, 0.99) AS imb_p99
                FROM base_result
                GROUP BY session
            """
            perc_df = base_rel.query("base_result", perc_query).fetchdf()
            
        except Exception as e:
            logger.error(f"[Profiler] DuckDB execution failed: {e}")
            raise

        if perc_df.empty:
            return {"error": "No historical data found."}

        return self._generate_empirical_percentiles(sig_df, perc_df)

    def _get_fallback_thresholds(self, sess: str) -> dict:
        fallbacks = {
            "ASIAN":     {"vol_percentiles": {"90": 20, "75": 10}, "imb_percentiles": {"90": 10, "75": 5}},
            "LONDON":    {"vol_percentiles": {"90": 35, "75": 20}, "imb_percentiles": {"90": 20, "75": 10}},
            "NEW_YORK":  {"vol_percentiles": {"90": 50, "75": 30}, "imb_percentiles": {"90": 30, "75": 15}},
            "OFF_HOURS": {"vol_percentiles": {"90": 15, "75": 5},  "imb_percentiles": {"90": 5,  "75": 2}}
        }
        return fallbacks.get(sess, fallbacks["OFF_HOURS"])

    def _generate_empirical_percentiles(self, sig_df, perc_df) -> dict:
        """
        Monta o profile a partir das agregações colunares do DuckDB.
        """
        profile = {
            "metadata": {"generated_at": datetime.now().isoformat(), "type": "empirical_percentiles_duckdb"},
            "signatures": {},
            "thresholds": {}
        }

        # Estatísticas de Win Rate por Assinatura e Sessão
        for _, row in sig_df.iterrows():
            sig = row['behavior_signature']
            sess = row['session']
            count = row['cluster_count']
            if count == 0: continue
            
            wins = row['wins']
            gross_profit = row['total_gross_profit']
            gross_loss = row['total_gross_loss']
            pf = gross_profit / gross_loss if gross_loss > 0 else float('inf')
            
            profile["signatures"][f"{sig}_{sess}"] = {
                "count": int(count),
                "win_rate": round(wins / count, 3),
                "profit_factor": round(pf, 2),
                "avg_mfe": round(row['avg_mfe'], 5)
            }

        # Tabelas de Percentis para o Motor em Tempo Real
        MIN_SAMPLES_FOR_PERCENTILES = 100
        
        for _, row in perc_df.iterrows():
            sess = row['session']
            count = row['session_count']
            
            if count < MIN_SAMPLES_FOR_PERCENTILES:
                logger.warning(f"[Profiler] Sessão {sess} tem apenas {count} amostras; usando fallback thresholds")
                profile["thresholds"][sess] = self._get_fallback_thresholds(sess)
                continue
            
            profile["thresholds"][sess] = {
                "vol_percentiles": {
                    "50": float(row['vol_p50']), "75": float(row['vol_p75']), 
                    "90": float(row['vol_p90']), "95": float(row['vol_p95']), "99": float(row['vol_p99'])
                },
                "imb_percentiles": {
                    "50": float(row['imb_p50']), "75": float(row['imb_p75']), 
                    "90": float(row['imb_p90']), "95": float(row['imb_p95']), "99": float(row['imb_p99'])
                },
            }

        return profile

    def save_profile(self, profile: dict, path: str = "profile.json"):
        with open(path, 'w') as f:
            json.dump(profile, f, indent=2, default=str)
        logger.info(f"[Profiler] Empirical Profile saved to {path}")
