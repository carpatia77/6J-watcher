import duckdb
import json
import logging
from datetime import datetime, timedelta
from typing import Optional
from config import Config

logger = logging.getLogger(__name__)


class SignatureProfiler:
    """
    Calcula MFE/MAE via DuckDB CTE e gera Tabelas de Percentis Empíricos
    para normalização não-paramétrica de Order Flow.
    Calibrado para micro-janelas de 250ms (ingestion.py _WINDOW_NS).
    """

    def __init__(self, db_path: str, cfg: Optional[Config] = None, conn=None):
        self.cfg = cfg or Config()
        self.db_path = db_path
        self.conn = conn if conn else duckdb.connect(db_path, read_only=True)

    def define_session(self, hour: int) -> str:
        if 0 <= hour < 8:   return "ASIAN"
        if 8 <= hour < 13:  return "LONDON"
        if 13 <= hour < 22: return "NEW_YORK"
        return "OFF_HOURS"

    def build_profile(
        self,
        symbol: str,
        lookback_days: int = 30,
        horizon_minutes: int = 30,
        tick_size: Optional[float] = None,
        since: Optional[str] = None,
        filter_dates: Optional[list] = None,
    ) -> dict:
        if not isinstance(horizon_minutes, int) or not (1 <= horizon_minutes <= 1440):
            raise ValueError("horizon_minutes must be an int between 1 and 1440")

        tick_size       = tick_size or self.cfg.tick_size
        horizon_ns      = horizon_minutes * 60 * 1_000_000_000
        interval_clause = f"INTERVAL '{int(horizon_minutes)}' MINUTE"

        if since:
            anchor_dt = datetime.strptime(since, "%Y-%m-%d")
            cutoff = (anchor_dt - timedelta(days=lookback_days)).strftime("%Y-%m-%d %H:%M:%S")
        else:
            cutoff = (datetime.now() - timedelta(days=lookback_days)).strftime("%Y-%m-%d %H:%M:%S")

        sym_safe    = symbol.replace("'", "")
        cutoff_safe = cutoff.replace("'", "")

        filter_clause = ""
        if filter_dates:
            dates_str = ",".join([f"'{str(d).replace(chr(39), '')}' " for d in filter_dates])
            filter_clause = f"AND CAST(c.timestamp AS DATE) IN ({dates_str})"

        from queries import build_mfe_mae_cte
        
        # O signature_profiler precisa da tabela 'scored', entao chamamos build_mfe_mae_cte
        # que ja retorna exatamente ate a CTE 'scored'.
        base_cte = build_mfe_mae_cte(
            symbol=sym_safe,
            start_date=cutoff_safe,
            horizon_minutes=horizon_minutes,
            is_sampling=True,
            sample_size=20000,
            filter_dates=filter_dates
        )

        full_query = base_cte + f"""
        ,
        sig_stats AS (
            SELECT
                behavior_signature || '_' || regime AS behavior_signature,
                session,
                COUNT(*)           AS cluster_count,
                SUM(win)           AS wins,
                SUM(gross_profit)  AS total_gross_profit,
                SUM(gross_loss)    AS total_gross_loss,
                AVG(mfe)           AS avg_mfe,
                AVG(mae)           AS avg_mae
            FROM scored
            GROUP BY behavior_signature || '_' || regime, session
        ),
        perc_stats AS (
            SELECT
                session,
                COUNT(*)                        AS session_count,
                QUANTILE_CONT(total_vol, 0.50)  AS vol_p50,
                QUANTILE_CONT(total_vol, 0.75)  AS vol_p75,
                QUANTILE_CONT(total_vol, 0.90)  AS vol_p90,
                QUANTILE_CONT(total_vol, 0.95)  AS vol_p95,
                QUANTILE_CONT(total_vol, 0.99)  AS vol_p99,
                QUANTILE_CONT(imbalance,  0.50) AS imb_p50,
                QUANTILE_CONT(imbalance,  0.75) AS imb_p75,
                QUANTILE_CONT(imbalance,  0.90) AS imb_p90,
                QUANTILE_CONT(imbalance,  0.95) AS imb_p95,
                QUANTILE_CONT(imbalance,  0.99) AS imb_p99
            FROM scored
            GROUP BY session
        )
        SELECT
            'sig'                  AS result_type,
            behavior_signature,    
            session,
            cluster_count,         wins,
            total_gross_profit,    total_gross_loss,  
            avg_mfe, avg_mae,
            NULL::DOUBLE           AS session_count,
            NULL::DOUBLE AS vol_p50, NULL::DOUBLE AS vol_p75, NULL::DOUBLE AS vol_p90,
            NULL::DOUBLE AS vol_p95, NULL::DOUBLE AS vol_p99,
            NULL::DOUBLE AS imb_p50, NULL::DOUBLE AS imb_p75, NULL::DOUBLE AS imb_p90,
            NULL::DOUBLE AS imb_p95, NULL::DOUBLE AS imb_p99
        FROM sig_stats
        UNION ALL
        SELECT
            'perc'                 AS result_type,
            NULL                   AS behavior_signature,  session,
            NULL::BIGINT           AS cluster_count,       NULL::BIGINT AS wins,
            NULL::DOUBLE           AS total_gross_profit,  NULL::DOUBLE AS total_gross_loss,
            NULL::DOUBLE           AS avg_mfe,             NULL::DOUBLE AS avg_mae,
            session_count,
            vol_p50, vol_p75, vol_p90, vol_p95, vol_p99,
            imb_p50, imb_p75, imb_p90, imb_p95, imb_p99
        FROM perc_stats
        """

        try:
            rows = self.conn.execute(full_query).fetchdf()
        except Exception as e:
            logger.error("[Profiler] DuckDB execution failed: %s", e)
            raise

        sig_df  = rows[rows["result_type"] == "sig"].copy()
        perc_df = rows[rows["result_type"] == "perc"].copy()

        if perc_df.empty:
            return {"error": "No historical data found."}

        # Calcula multipliers de profundidade (win_rate por banda relativo ao mid)
        depth_sql = base_cte + """
        ,
        win_by_band AS (
            SELECT
                behavior_signature AS sig,
                CASE 
                    WHEN dom_min_level <= 2 THEN 'shallow'
                    WHEN dom_min_level <= 5 THEN 'mid'
                    ELSE 'deep'
                END AS depth_band,
                COUNT(*) AS cnt,
                SUM(win) * 1.0 / COUNT(*) AS win_rate
            FROM scored
            GROUP BY behavior_signature, depth_band
        )
        SELECT sig, depth_band, cnt, win_rate
        FROM win_by_band
        ORDER BY sig, depth_band
        """
        try:
            depth_rows = self.conn.execute(depth_sql).fetchall()
        except Exception as e:
            logger.error("[Profiler] Depth SQL execution failed: %s", e)
            raise

        raw_bands = {}
        for (sig, band, cnt, wr) in depth_rows:
            raw_bands.setdefault(sig, {})[band] = {"cnt": int(cnt), "win_rate": float(wr or 0.0)}

        MIN_DEPTH_SAMPLES = 50
        depth_multipliers = {}
        has_depth = False

        for sig, bands in raw_bands.items():
            mid_wr = bands.get("mid", {}).get("win_rate", 0.0)
            if mid_wr == 0.0:
                continue

            sig_mults = {}
            for band in ("shallow", "mid", "deep"):
                info = bands.get(band, {})
                if info.get("cnt", 0) < MIN_DEPTH_SAMPLES:
                    continue
                raw_mult = info["win_rate"] / mid_wr
                sig_mults[band] = round(max(0.5, min(2.0, raw_mult)), 4)

            if sig_mults:
                sig_mults["mid"] = 1.0
                depth_multipliers[sig] = sig_mults
                has_depth = True
                logger.info("[Profiler] depth_multipliers %s: %s", sig, sig_mults)

        return self._generate_empirical_percentiles(sig_df, perc_df, depth_multipliers, has_depth)

    def _get_fallback_thresholds(self, sess: str) -> dict:
        """
        Limiares calibrados para janelas de 250ms do 6J CME MBP-10.
        Substitui valores de tick-único anteriores (5-11 lotes) que causavam
        vol_p >= 90 em quase toda janela, colapsando tudo em ABSORPTION_PASSIVE.

        Estimativas para distribuição típica de volume por janela de 250ms:
          ASIAN:     10-80  lotes  (baixo volume pré-LONDON)
          LONDON:    50-200 lotes  (abertura europeia)
          NEW_YORK:  80-300 lotes  (overlap NY/London, pico de liquidez 6J)
          OFF_HOURS: 5-60   lotes  (mínimo pós-fechamento NY)
        """
        fallbacks = {
            "ASIAN": {
                "vol_percentiles": {"50": 20,  "75": 40,  "90": 80,  "95": 120, "99": 200},
                "imb_percentiles": {"50": 8,   "75": 15,  "90": 30,  "95": 50,  "99": 80},
            },
            "LONDON": {
                "vol_percentiles": {"50": 50,  "75": 100, "90": 200, "95": 300, "99": 500},
                "imb_percentiles": {"50": 20,  "75": 40,  "90": 80,  "95": 120, "99": 200},
            },
            "NEW_YORK": {
                "vol_percentiles": {"50": 70,  "75": 150, "90": 300, "95": 450, "99": 700},
                "imb_percentiles": {"50": 30,  "75": 60,  "90": 120, "95": 180, "99": 300},
            },
            "OFF_HOURS": {
                "vol_percentiles": {"50": 15,  "75": 30,  "90": 60,  "95": 90,  "99": 150},
                "imb_percentiles": {"50": 5,   "75": 10,  "90": 20,  "95": 35,  "99": 60},
            },
        }
        return fallbacks.get(sess, fallbacks["OFF_HOURS"])

    def _generate_empirical_percentiles(self, sig_df, perc_df, depth_multipliers: dict, has_depth: bool) -> dict:
        profile = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "type":         "empirical_percentiles_duckdb",
                "window_ms":    250,
                "has_depth_calibration": has_depth,
            },
            "signatures": {},
            "thresholds": {},
            "depth_multipliers": depth_multipliers,
        }

        for _, row in sig_df.iterrows():
            sig   = row["behavior_signature"]
            sess  = row["session"]
            count = row["cluster_count"]
            if not sig or not sess or not count or count == 0:
                continue
            wins         = row["wins"] or 0
            gross_profit = row["total_gross_profit"] or 0
            gross_loss = float(row["total_gross_loss"] or 0)
            
            profit_factor = (
                gross_profit / gross_loss
                if gross_loss > 1e-10
                else float('inf')
            )
            
            # Skip statistically insignificant signatures
            if count < 30:
                continue
                
            profile["signatures"][f"{sig}_{sess}"] = {
                "count":         int(count),
                "win_rate":      round(wins / count, 3),
                "profit_factor": round(profit_factor, 2),
                "avg_mfe":       round(row["avg_mfe"] or 0, 7),
            }

        MIN_SAMPLES = 100
        for _, row in perc_df.iterrows():
            sess  = row["session"]
            count = row["session_count"]
            if not sess:
                continue
            if not count or count < MIN_SAMPLES:
                logger.warning(
                    "[Profiler] Sessao %s com %s amostras (< %d) — usando fallback 250ms",
                    sess, count, MIN_SAMPLES,
                )
                profile["thresholds"][sess] = self._get_fallback_thresholds(sess)
                continue
            profile["thresholds"][sess] = {
                "vol_percentiles": {
                    "50": float(row["vol_p50"]), "75": float(row["vol_p75"]),
                    "90": float(row["vol_p90"]), "95": float(row["vol_p95"]),
                    "99": float(row["vol_p99"]),
                },
                "imb_percentiles": {
                    "50": float(row["imb_p50"]), "75": float(row["imb_p75"]),
                    "90": float(row["imb_p90"]), "95": float(row["imb_p95"]),
                    "99": float(row["imb_p99"]),
                },
            }

        return profile

    def save_profile(self, profile: dict, path: str = "profile.json"):
        with open(path, "w") as f:
            json.dump(profile, f, indent=2, default=str)
        logger.info("[Profiler] Empirical Profile saved to %s", path)
