from __future__ import annotations
"""
signature_profiler.py
---------------------
Calcula MFE/MAE histórico e percentis de volume/desequilíbrio por sessão.
Gera profile_calibrated.json consumido pelo AdaptivePatternEngine.

A partir do MEDIO-08, estratifica também por depth_band (shallow/mid/deep)
e gera depth_multipliers empíricos ancorando mid=1.0.

O profile.json com has_depth_calibration=true indica que depth_multipliers
foram gerados e o engine os aplicará automaticamente no classify().
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


MIN_SAMPLES       = 100   # mínimo para usar percentis empíricos (vs fallback)
MIN_DEPTH_SAMPLES = 30    # mínimo por depth_band para gerar multiplier
FUTURE_WINDOW_NS  = 30_000_000_000   # 30 segundos em ns


class SignatureProfiler:
    def __init__(self, repo, profile_path: str, symbol: str = "6J"):
        self.repo         = repo
        self.profile_path = profile_path
        self.symbol       = symbol

    # ── Fallback thresholds (quando sessão tem < MIN_SAMPLES) ──────────────────

    def _get_fallback_thresholds(self) -> Dict:
        """Calibrado para janelas de 250ms do 6J CME MBP-10."""
        return {
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

    # ── Build principal ────────────────────────────────────────────────────────────────

    def build_profile(
        self,
        filter_dates: Optional[List[str]] = None,
        output_path:  Optional[str]       = None,
    ) -> dict:
        """
        Gera profile_calibrated.json a partir dos clusters no DuckDB.

        filter_dates: lista de strings 'YYYY-MM-DD' para restringir o período.
        output_path:  sobrescreve self.profile_path se fornecido.

        Estrutura do output:
        {
          "metadata":           { generated_at, symbol, window_ms, has_depth_calibration },
          "thresholds":         { SESSION: { vol_percentiles, imb_percentiles } },
          "signatures":         { "sig_SESSION": { win_rate, profit_factor, count } },
          "depth_multipliers":  { "sig": { "shallow": float, "mid": 1.0, "deep": float } }
        }
        """
        date_filter = ""
        if filter_dates:
            safe = [d.replace(chr(39), "") for d in filter_dates]
            placeholders = ",".join(f"'{d}'" for d in safe)
            date_filter = f"AND DATE(lc.timestamp) IN ({placeholders})"

        # ── 1. Percentis de volume e desequilíbrio por sessão ────────────────────
        threshold_sql = f"""
        SELECT
            session,
            COUNT(*)                                          AS n,
            QUANTILE_CONT(total_bid + total_ask, 0.50)        AS vol_p50,
            QUANTILE_CONT(total_bid + total_ask, 0.75)        AS vol_p75,
            QUANTILE_CONT(total_bid + total_ask, 0.90)        AS vol_p90,
            QUANTILE_CONT(total_bid + total_ask, 0.95)        AS vol_p95,
            QUANTILE_CONT(total_bid + total_ask, 0.99)        AS vol_p99,
            QUANTILE_CONT(ABS(total_bid - total_ask), 0.50)   AS imb_p50,
            QUANTILE_CONT(ABS(total_bid - total_ask), 0.75)   AS imb_p75,
            QUANTILE_CONT(ABS(total_bid - total_ask), 0.90)   AS imb_p90,
            QUANTILE_CONT(ABS(total_bid - total_ask), 0.95)   AS imb_p95,
            QUANTILE_CONT(ABS(total_bid - total_ask), 0.99)   AS imb_p99
        FROM liquidity_clusters lc
        WHERE symbol = '{self.symbol}' {date_filter}
        GROUP BY session
        """
        thresh_rows = self.repo.conn.execute(threshold_sql).fetchall()

        fallback = self._get_fallback_thresholds()
        thresholds: Dict = {}
        for row in thresh_rows:
            (session, n,
             vp50, vp75, vp90, vp95, vp99,
             ip50, ip75, ip90, ip95, ip99) = row
            if n < MIN_SAMPLES:
                logging.warning("[Profiler] Sessão %s com %d amostras — usando fallback.", session, n)
                thresholds[session] = fallback.get(session, fallback["OFF_HOURS"])
            else:
                thresholds[session] = {
                    "vol_percentiles": {"50": vp50, "75": vp75, "90": vp90, "95": vp95, "99": vp99},
                    "imb_percentiles": {"50": ip50, "75": ip75, "90": ip90, "95": ip95, "99": ip99},
                }
        for sess in fallback:
            if sess not in thresholds:
                thresholds[sess] = fallback[sess]

        # ── 2. MFE/MAE por (assinatura, sessão) ───────────────────────────────────
        mfe_sql = f"""
        WITH future AS (
            SELECT
                lc.symbol,
                lc.timestamp_ns                        AS c_ts_ns,
                lc.price                               AS c_price,
                lc.behavior_signature                  AS sig,
                lc.session,
                MAX(te.price)                          AS max_future,
                MIN(te.price)                          AS min_future
            FROM liquidity_clusters lc
            JOIN tape_events te
                ON  te.symbol       = lc.symbol
                AND (
                    CASE
                        WHEN lc.timestamp_ns IS NOT NULL AND te.timestamp_ns IS NOT NULL
                        THEN te.timestamp_ns BETWEEN lc.timestamp_ns
                                                 AND lc.timestamp_ns + {FUTURE_WINDOW_NS}
                        ELSE te.timestamp BETWEEN lc.timestamp
                                              AND lc.timestamp + INTERVAL 30 SECOND
                    END
                )
            WHERE lc.symbol = '{self.symbol}' {date_filter}
              AND lc.behavior_signature <> 'unknown'
            GROUP BY lc.symbol, lc.timestamp_ns, lc.price, lc.behavior_signature, lc.session
        ),
        metrics AS (
            SELECT
                sig,
                session,
                CASE
                    WHEN sig IN ('iceberg_accumulation','breakout_genuine','defense_line')
                         THEN max_future - c_price
                    WHEN sig IN ('iceberg_distribution','absorption_passive')
                         THEN c_price - min_future
                    ELSE GREATEST(max_future - c_price, c_price - min_future)
                END AS mfe,
                CASE
                    WHEN sig IN ('iceberg_accumulation','breakout_genuine','defense_line')
                         THEN c_price - min_future
                    WHEN sig IN ('iceberg_distribution','absorption_passive')
                         THEN max_future - c_price
                    ELSE 0
                END AS mae
            FROM future
        )
        SELECT
            sig,
            session,
            COUNT(*)                   AS cnt,
            AVG(mfe)                   AS avg_mfe,
            AVG(mae)                   AS avg_mae,
            SUM(CASE WHEN mfe > mae THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS win_rate,
            CASE WHEN AVG(mae) > 0 THEN AVG(mfe) / AVG(mae) ELSE 0 END  AS profit_factor
        FROM metrics
        GROUP BY sig, session
        ORDER BY sig, session
        """
        sig_rows = self.repo.conn.execute(mfe_sql).fetchall()

        signatures: Dict = {}
        for (sig, session, cnt, avg_mfe, avg_mae, win_rate, pf) in sig_rows:
            key = f"{sig}_{session}"
            signatures[key] = {
                "win_rate":      round(win_rate or 0.0, 4),
                "profit_factor": round(pf      or 0.0, 4),
                "avg_mfe":       round(avg_mfe  or 0.0, 6),
                "avg_mae":       round(avg_mae  or 0.0, 6),
                "count":         int(cnt),
            }

        # ── 3. Depth multipliers por (assinatura, depth_band) ──────────────────────
        # Apenas para assinaturas depth-sensitive: spoofing_wall, iceberg_*
        # Ancora mid=1.0 para que o multiplier seja relativo, não absoluto.
        # Se depth_band não tem MIN_DEPTH_SAMPLES, não gera multiplier (engine usa 1.0).
        depth_sql = f"""
        WITH future AS (
            SELECT
                lc.symbol,
                lc.timestamp_ns                        AS c_ts_ns,
                lc.price                               AS c_price,
                lc.behavior_signature                  AS sig,
                lc.session,
                lc.dom_min_level,
                CASE
                    WHEN lc.dom_min_level <= 2 THEN 'shallow'
                    WHEN lc.dom_min_level <= 5 THEN 'mid'
                    ELSE 'deep'
                END                                    AS depth_band,
                MAX(te.price)                          AS max_future,
                MIN(te.price)                          AS min_future
            FROM liquidity_clusters lc
            JOIN tape_events te
                ON  te.symbol = lc.symbol
                AND (
                    CASE
                        WHEN lc.timestamp_ns IS NOT NULL AND te.timestamp_ns IS NOT NULL
                        THEN te.timestamp_ns BETWEEN lc.timestamp_ns
                                                 AND lc.timestamp_ns + {FUTURE_WINDOW_NS}
                        ELSE te.timestamp BETWEEN lc.timestamp
                                              AND lc.timestamp + INTERVAL 30 SECOND
                    END
                )
            WHERE lc.symbol      = '{self.symbol}' {date_filter}
              AND lc.dom_min_level IS NOT NULL
              AND lc.behavior_signature IN (
                  'spoofing_wall', 'iceberg_accumulation', 'iceberg_distribution'
              )
            GROUP BY lc.symbol, lc.timestamp_ns, lc.price,
                     lc.behavior_signature, lc.session, lc.dom_min_level
        ),
        metrics AS (
            SELECT
                sig,
                depth_band,
                CASE
                    WHEN sig IN ('iceberg_accumulation')
                         THEN max_future - c_price
                    WHEN sig IN ('iceberg_distribution')
                         THEN c_price - min_future
                    ELSE GREATEST(max_future - c_price, c_price - min_future)
                END AS mfe,
                CASE
                    WHEN sig IN ('iceberg_accumulation')
                         THEN c_price - min_future
                    WHEN sig IN ('iceberg_distribution')
                         THEN max_future - c_price
                    ELSE 0
                END AS mae
            FROM future
        ),
        win_by_band AS (
            SELECT
                sig,
                depth_band,
                COUNT(*)                                                      AS cnt,
                SUM(CASE WHEN mfe > mae THEN 1 ELSE 0 END) * 1.0 / COUNT(*)  AS win_rate
            FROM metrics
            GROUP BY sig, depth_band
        )
        SELECT sig, depth_band, cnt, win_rate
        FROM win_by_band
        ORDER BY sig, depth_band
        """
        depth_rows = self.repo.conn.execute(depth_sql).fetchall()

        # Agrega por sig: calcula multiplier relativo ao mid
        raw_bands: Dict[str, Dict[str, Dict]] = {}
        for (sig, band, cnt, wr) in depth_rows:
            raw_bands.setdefault(sig, {})[band] = {"cnt": int(cnt), "win_rate": float(wr or 0.0)}

        depth_multipliers: Dict[str, Dict[str, float]] = {}
        has_depth = False

        for sig, bands in raw_bands.items():
            mid_wr = bands.get("mid", {}).get("win_rate", 0.0)
            if mid_wr == 0.0:
                continue  # sem ancora, não gera multiplier

            sig_mults: Dict[str, float] = {}
            for band in ("shallow", "mid", "deep"):
                info = bands.get(band, {})
                if info.get("cnt", 0) < MIN_DEPTH_SAMPLES:
                    continue  # amostras insuficientes — engine usa 1.0
                raw_mult = info["win_rate"] / mid_wr
                # Clamp [0.5, 2.0] para evitar multiplicadores explosivos
                sig_mults[band] = round(max(0.5, min(2.0, raw_mult)), 4)

            if sig_mults:
                # Garante que mid=1.0 exatamente (ancora)
                sig_mults["mid"] = 1.0
                depth_multipliers[sig] = sig_mults
                has_depth = True
                logging.info(
                    "[Profiler] depth_multipliers %s: %s",
                    sig, sig_mults
                )

        # ── 4. Monta e persiste o profile ──────────────────────────────────────────
        profile = {
            "metadata": {
                "generated_at":          datetime.utcnow().isoformat(),
                "symbol":                self.symbol,
                "window_ms":             250,
                "has_depth_calibration": has_depth,
            },
            "thresholds":        thresholds,
            "signatures":        signatures,
            "depth_multipliers": depth_multipliers,
        }

        out_path = output_path or self.profile_path
        Path(out_path).parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w") as f:
            json.dump(profile, f, indent=2, default=str)

        logging.info(
            "[Profiler] profile salvo em %s | %d assinaturas | depth_calibration=%s",
            out_path, len(signatures), has_depth
        )
        return profile
