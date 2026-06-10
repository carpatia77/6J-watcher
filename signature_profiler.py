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

        full_query = f"""
        WITH cluster_excursions AS (
            SELECT
                c.timestamp,
                c.timestamp_ns,
                c.behavior_signature,
                c.session,
                (c.total_bid + c.total_ask)            AS total_vol,
                c.total_bid,
                c.total_ask,
                c.cumdelta,
                ABS(c.total_bid - c.total_ask)         AS imbalance,
                c.price                                AS c_price,
                COALESCE(MAX(t.price), c.price)        AS max_future_price,
                COALESCE(MIN(t.price), c.price)        AS min_future_price
            FROM liquidity_clusters c USING SAMPLE 20000 ROWS
            LEFT JOIN tape_events t
              ON  c.symbol = t.symbol
              AND (
                CASE WHEN c.timestamp_ns IS NOT NULL AND t.timestamp_ns IS NOT NULL
                     THEN t.timestamp_ns > c.timestamp_ns
                          AND t.timestamp_ns <= c.timestamp_ns + (CASE WHEN c.behavior_signature = 'spoofing_wall' THEN 120000000000 ELSE {horizon_ns} END)
                     ELSE t.timestamp > c.timestamp
                          AND t.timestamp <= c.timestamp + INTERVAL 1 MINUTE * (CASE WHEN c.behavior_signature = 'spoofing_wall' THEN 2 ELSE {self.horizon_minutes} END)
                END
              )
            WHERE c.symbol = '{sym_safe}'
              AND c.timestamp > '{cutoff_safe}'
              {filter_clause}
            GROUP BY c.timestamp, c.timestamp_ns, c.behavior_signature, c.session,
                     c.total_bid, c.total_ask, c.cumdelta, c.price
        ),
        mfe_mae_calc AS (
            SELECT *,
                -- P3: cada assinatura mapeada para sua direção esperada.
                -- DEFENSE_LINE estava incorretamente no ELSE (bearish) antes deste fix.
                -- spoofing_wall e liquidity_vacuum são neutros: usa excursão máxima.
                CASE
                    WHEN behavior_signature IN (
                        'iceberg_accumulation', 'breakout_genuine', 'defense_line'
                    ) THEN max_future_price - c_price
                    WHEN behavior_signature IN (
                        'iceberg_distribution', 'absorption_passive'
                    ) THEN c_price - min_future_price
                    WHEN behavior_signature IN ('spoofing_wall', 'liquidity_vacuum') AND total_bid > total_ask THEN max_future_price - c_price
                    WHEN behavior_signature IN ('spoofing_wall', 'liquidity_vacuum') AND total_ask > total_bid THEN c_price - min_future_price
                    ELSE GREATEST(
                        max_future_price - c_price,
                        c_price - min_future_price
                    )
                END AS mfe,
                CASE
                    WHEN behavior_signature IN (
                        'iceberg_accumulation', 'breakout_genuine', 'defense_line'
                    ) THEN c_price - min_future_price
                    WHEN behavior_signature IN (
                        'iceberg_distribution', 'absorption_passive'
                    ) THEN max_future_price - c_price
                    WHEN behavior_signature IN ('spoofing_wall', 'liquidity_vacuum') AND total_bid > total_ask THEN c_price - min_future_price
                    WHEN behavior_signature IN ('spoofing_wall', 'liquidity_vacuum') AND total_ask > total_bid THEN max_future_price - c_price
                    ELSE GREATEST(
                        max_future_price - c_price,
                        c_price - min_future_price
                    )
                END AS mae
            FROM cluster_excursions
        ),
        scored AS (
            SELECT *,
                CASE WHEN mfe > ABS(mae) AND mfe > 0 THEN 1 ELSE 0 END AS win,
                CASE WHEN mfe > 0   THEN mfe      ELSE 0 END           AS gross_profit,
                CASE WHEN mae < 0   THEN ABS(mae) ELSE 0 END           AS gross_loss
            FROM mfe_mae_calc
        ),
        sig_stats AS (
            SELECT
                behavior_signature,
                session,
                COUNT(*)           AS cluster_count,
                SUM(win)           AS wins,
                SUM(gross_profit)  AS total_gross_profit,
                SUM(gross_loss)    AS total_gross_loss,
                AVG(mfe)           AS avg_mfe
            FROM scored
            GROUP BY behavior_signature, session
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
            behavior_signature,    session,
            cluster_count,         wins,
            total_gross_profit,    total_gross_loss,  avg_mfe,
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
            NULL::DOUBLE           AS avg_mfe,
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

        return self._generate_empirical_percentiles(sig_df, perc_df)

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

    def _generate_empirical_percentiles(self, sig_df, perc_df) -> dict:
        profile = {
            "metadata": {
                "generated_at": datetime.now().isoformat(),
                "type":         "empirical_percentiles_duckdb",
                "window_ms":    250,
            },
            "signatures": {},
            "thresholds": {},
        }

        for _, row in sig_df.iterrows():
            sig   = row["behavior_signature"]
            sess  = row["session"]
            count = row["cluster_count"]
            if not sig or not sess or not count or count == 0:
                continue
            wins         = row["wins"] or 0
            gross_profit = row["total_gross_profit"] or 0
            gross_loss   = row["total_gross_loss"]   or 0
            pf = (gross_profit / gross_loss) if gross_loss and gross_loss > 0 else float("inf")
            profile["signatures"][f"{sig}_{sess}"] = {
                "count":         int(count),
                "win_rate":      round(wins / count, 3),
                "profit_factor": round(pf, 2),
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
