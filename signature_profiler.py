import duckdb
import numpy as np
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

logger = logging.getLogger(__name__)

class SignatureProfiler:
    """
    Calcula MFE/MAE via DuckDB Window Functions e gera Tabelas de Percentis
    Empíricos para normalização não-paramétrica de Order Flow.
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path, read_only=True)

    def define_session(self, hour: int) -> str:
        if 0 <= hour < 8: return "ASIAN"
        if 8 <= hour < 13: return "LONDON"
        if 13 <= hour < 22: return "NEW_YORK"
        return "OFF_HOURS"

    def build_profile(self, symbol: str, lookback_days: int = 30, horizon_minutes: int = 30, tick_size: float = 0.00005) -> dict:
        cutoff = datetime.now() - timedelta(days=lookback_days)
        
        # 1. CÁLCULO VETORIAL DE MFE/MAE NO DUCKDB (Substitui o loop O(N^2) do Python)
        # Usamos Window Functions com RANGE BETWEEN para olhar o futuro de cada cluster.
        mfe_mae_query = f"""
        WITH cluster_excursions AS (
            SELECT 
                c.timestamp,
                c.price AS c_price,
                c.delta_price_ticks,
                c.behavior_signature,
                c.session,
                c.total_bid,
                c.total_ask,
                (c.total_bid + c.total_ask) AS total_vol,
                ABS(c.total_bid - c.total_ask) AS imbalance,
                MAX(t.price) OVER (
                    PARTITION BY c.symbol 
                    ORDER BY t.timestamp 
                    RANGE BETWEEN CURRENT ROW AND INTERVAL '{horizon_minutes}' MINUTE FOLLOWING
                ) AS max_future_price,
                MIN(t.price) OVER (
                    PARTITION BY c.symbol 
                    ORDER BY t.timestamp 
                    RANGE BETWEEN CURRENT ROW AND INTERVAL '{horizon_minutes}' MINUTE FOLLOWING
                ) AS min_future_price
            FROM liquidity_clusters c
            LEFT JOIN tape_events t 
              ON c.symbol = t.symbol 
             AND t.timestamp >= c.timestamp 
             AND t.timestamp <= c.timestamp + INTERVAL '{horizon_minutes}' MINUTE
            WHERE c.symbol = ? AND c.timestamp > ?
        )
        SELECT 
            timestamp, c_price, delta_price_ticks, behavior_signature, session, 
            total_vol, imbalance, max_future_price, min_future_price
        FROM cluster_excursions
        """
        
        try:
            df = self.conn.execute(mfe_mae_query, [symbol, cutoff]).fetchdf()
        except Exception as e:
            logger.error(f"[Profiler] DuckDB execution failed: {e}")
            raise

        if df.empty:
            return {"error": "No historical data found."}

        # 2. CÁLCULO DE WIN/LOSS BASEADO NA DIRECIONALIDADE DA ASSINATURA
        bullish_sigs = ['ICEBERG_ACCUMULATION', 'BREAKOUT_GENUINE', 'MAGNET_EFFECT']
        
        df['is_bullish'] = df['behavior_signature'].isin(bullish_sigs)
        df['mfe'] = np.where(df['is_bullish'], df['max_future_price'].fillna(df['c_price']) - df['c_price'], df['c_price'] - df['min_future_price'].fillna(df['c_price']))
        df['mae'] = np.where(df['is_bullish'], df['c_price'] - df['min_future_price'].fillna(df['c_price']), df['max_future_price'].fillna(df['c_price']) - df['c_price'])
        df['win'] = (df['mfe'] > np.abs(df['mae'])) & (df['mfe'] > 0)

        return self._generate_empirical_percentiles(df)

    def _get_fallback_thresholds(self, sess: str) -> dict:
        fallbacks = {
            "ASIAN":     {"vol_percentiles": {"90": 20, "75": 10}, "imb_percentiles": {"90": 10, "75": 5}},
            "LONDON":    {"vol_percentiles": {"90": 35, "75": 20}, "imb_percentiles": {"90": 20, "75": 10}},
            "NEW_YORK":  {"vol_percentiles": {"90": 50, "75": 30}, "imb_percentiles": {"90": 30, "75": 15}},
            "OFF_HOURS": {"vol_percentiles": {"90": 15, "75": 5},  "imb_percentiles": {"90": 5,  "75": 2}}
        }
        return fallbacks.get(sess, fallbacks["OFF_HOURS"])

    def _generate_empirical_percentiles(self, df) -> dict:
        """
        Substitui Média/StdDev por Percentis Empíricos (Rank Normalization).
        Imune a outliers (spikes de volume no Payroll).
        """
        profile = {
            "metadata": {"generated_at": datetime.now().isoformat(), "type": "empirical_percentiles"},
            "signatures": {},
            "thresholds": {}
        }

        # Estatísticas de Win Rate por Assinatura e Sessão
        for (sig, sess), group in df.groupby(['behavior_signature', 'session']):
            if group.empty: continue
            wins = group['win'].sum()
            gross_profit = group[group['mfe'] > 0]['mfe'].sum()
            gross_loss = abs(group[group['mae'] < 0]['mae'].sum())
            pf = gross_profit / gross_loss if gross_loss > 0 else float('inf')
            
            profile["signatures"][f"{sig}_{sess}"] = {
                "count": len(group),
                "win_rate": round(wins / len(group), 3),
                "profit_factor": round(pf, 2),
                "avg_mfe": round(group['mfe'].mean(), 5)
            }

        # Tabelas de Percentis para o Motor em Tempo Real
        percentiles_to_calc = [50, 75, 90, 95, 99]
        
        MIN_SAMPLES_FOR_PERCENTILES = 100
        
        for sess, group in df.groupby('session'):
            if group.empty: continue
            
            if len(group) < MIN_SAMPLES_FOR_PERCENTILES:
                logger.warning(f"[Profiler] Sessão {sess} tem apenas {len(group)} amostras; usando fallback thresholds")
                profile["thresholds"][sess] = self._get_fallback_thresholds(sess)
                continue
            
            # Calcula os percentis exatos para Volume, Imbalance e Adiciona Tick Displacement (Proxy)
            profile["thresholds"][sess] = {
                "vol_percentiles": {str(p): float(v) for p, v in zip(percentiles_to_calc, np.percentile(group['total_vol'], percentiles_to_calc))},
                "imb_percentiles": {str(p): float(v) for p, v in zip(percentiles_to_calc, np.percentile(group['imbalance'], percentiles_to_calc))},
            }

        return profile

    def save_profile(self, profile: dict, path: str = "profile.json"):
        with open(path, 'w') as f:
            json.dump(profile, f, indent=2, default=str)
        logger.info(f"[Profiler] Empirical Profile saved to {path}")
