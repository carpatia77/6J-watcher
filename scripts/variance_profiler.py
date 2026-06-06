"""
variance_profiler.py
--------------------
Gera perfil de variância diária para identificar:
  - Dias de operação institucional (Whale Days / Trend Days)
  - Dias choppy a excluir da calibração de thresholds
  - Padrões táticos recorrentes por dia-da-semana e mês

Uso (após recalibrate_clusters.py):
    python scripts/variance_profiler.py \
        --db ./data/backtest_8months.db \
        --out ./data/variance_report.json \
        --symbol 6J
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import json
import logging
import math
from datetime import datetime
from collections import defaultdict

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TICK_SIZE   = 0.00005
TIER_1_SIGS = {"absorption_passive", "breakout_genuine", "defense_line"}
TIER_2_SIGS = {"iceberg_accumulation", "iceberg_distribution", "magnet_effect"}
CHOPPY_SIGS = {"spoofing_wall", "liquidity_vacuum", "unknown"}

# Pesos do Whale Day Score
W_DIR   = 0.35   # Direcionalidade (cumdelta ratio)
W_TIER1 = 0.30   # Concentração Tática Tier-1
W_CI    = 0.20   # 1 - Choppiness Index
W_AMP   = 0.15   # Amplitude direcional

WDS_TREND_THRESHOLD = 0.60
WDS_NOISE_THRESHOLD = 0.35


def compute_daily_metrics(conn: duckdb.DuckDBPyConnection, symbol: str) -> list[dict]:
    """
    Agrega todas as métricas por dia em uma única query DuckDB.
    O Choppiness Index usa tape_events para range real do dia.
    """
    rows = conn.execute("""
        WITH daily_clusters AS (
            SELECT
                CAST(timestamp AS DATE)                     AS day,
                DAYOFWEEK(timestamp)                        AS dow,   -- 0=Sun, 1=Mon...
                EXTRACT(MONTH FROM timestamp)::INTEGER      AS month,
                behavior_signature,
                cumdelta,
                delta_price_ticks,
                total_bid + total_ask                       AS volume,
                ABS(total_bid - total_ask)                  AS imbalance
            FROM liquidity_clusters
            WHERE symbol = ?
        ),
        daily_tape AS (
            SELECT
                CAST(timestamp AS DATE)  AS day,
                MAX(price)               AS high,
                MIN(price)               AS low,
                SUM(volume)              AS tape_volume,
                COUNT(*)                 AS tape_count
            FROM tape_events
            WHERE symbol = ?
            GROUP BY CAST(timestamp AS DATE)
        ),
        daily_agg AS (
            SELECT
                dc.day,
                dc.dow,
                dc.month,
                COUNT(*)                                        AS total_clusters,
                SUM(dc.volume)                                  AS total_volume,
                SUM(ABS(dc.cumdelta))                           AS abs_cumdelta_sum,
                SUM(dc.cumdelta)                                AS net_cumdelta,
                -- Tier counts
                SUM(CASE WHEN dc.behavior_signature IN (
                    'absorption_passive','breakout_genuine','defense_line'
                ) THEN 1 ELSE 0 END)                            AS tier1_count,
                SUM(CASE WHEN dc.behavior_signature IN (
                    'iceberg_accumulation','iceberg_distribution'
                ) THEN 1 ELSE 0 END)                            AS tier2_count,
                SUM(CASE WHEN dc.behavior_signature IN (
                    'spoofing_wall','liquidity_vacuum','unknown'
                ) THEN 1 ELSE 0 END)                            AS choppy_count,
                -- Deslocamento direcional
                SUM(dc.delta_price_ticks)                       AS net_delta_ticks,
                SUM(ABS(dc.delta_price_ticks))                  AS abs_delta_ticks_sum,
                STDDEV(dc.delta_price_ticks)                    AS stddev_delta_ticks,
                -- Imbalance médio
                AVG(dc.imbalance)                               AS avg_imbalance,
                -- Assinaturas dominantes (top 3)
                MODE(dc.behavior_signature)                     AS dominant_sig
            FROM daily_clusters dc
            GROUP BY dc.day, dc.dow, dc.month
        )
        SELECT
            a.*,
            t.high,
            t.low,
            t.tape_volume,
            t.tape_count,
            -- Range do dia em ticks
            ROUND((t.high - t.low) / ?) AS range_ticks
        FROM daily_agg a
        LEFT JOIN daily_tape t ON a.day = t.day
        ORDER BY a.day ASC
    """, [symbol, symbol, TICK_SIZE]).fetchall()

    cols = [
        "day", "dow", "month", "total_clusters", "total_volume",
        "abs_cumdelta_sum", "net_cumdelta", "tier1_count", "tier2_count",
        "choppy_count", "net_delta_ticks", "abs_delta_ticks_sum",
        "stddev_delta_ticks", "avg_imbalance", "dominant_sig",
        "high", "low", "tape_volume", "tape_count", "range_ticks"
    ]
    return [dict(zip(cols, r)) for r in rows]


def compute_wds(d: dict) -> dict:
    """
    Calcula Whale Day Score e componentes para um dia.
    Retorna o dict enriquecido com scores e classificação.
    """
    total = max(d["total_clusters"], 1)
    vol   = max(d["total_volume"], 1)

    # ── Métrica 1: Direcionalidade ────────────────────────────────────
    # |net_cumdelta| / total_volume: 0 = sem direção, 1 = totalmente direcional
    dir_score = min(abs(d["net_cumdelta"] or 0) / vol, 1.0)

    # ── Métrica 2: Concentração Tier-1 ───────────────────────────────
    tier1_ratio = (d["tier1_count"] or 0) / total

    # ── Métrica 3: Choppiness Index ───────────────────────────────────
    # CI = log10(sum_abs_moves) / log10(range_total)
    # CI próximo de 1.0 = choppy, próximo de 0 = trending
    abs_moves = d["abs_delta_ticks_sum"] or 0
    range_t   = max(d["range_ticks"] or 1, 1)
    if abs_moves > 0 and range_t > 1:
        ci = math.log10(abs_moves) / math.log10(range_t)
        ci = max(0.0, min(ci, 1.0))   # clamp [0, 1]
    else:
        ci = 1.0   # sem movimento = máximo choppiness

    # ── Métrica 4: Amplitude Direcional ──────────────────────────────
    # Penaliza STDDEV alto sem direção consistente
    net_dir = abs(d["net_delta_ticks"] or 0)
    abs_sum = max(abs_moves, 1)
    amp_signed = net_dir / abs_sum  # 0 = zig-zag puro, 1 = move unidirecional

    # ── Whale Day Score ───────────────────────────────────────────────
    wds = (
        W_DIR   * dir_score   +
        W_TIER1 * tier1_ratio +
        W_CI    * (1.0 - ci)  +
        W_AMP   * amp_signed
    )

    # Classificação
    if wds >= WDS_TREND_THRESHOLD:
        day_class = "TREND_DAY"
    elif wds <= WDS_NOISE_THRESHOLD:
        day_class = "NOISE_DAY"
    else:
        day_class = "MIXED_DAY"

    # Direcionalidade qualitativa
    net_cd = d["net_cumdelta"] or 0
    if abs(net_cd) < vol * 0.05:
        direction = "NEUTRAL"
    elif net_cd > 0:
        direction = "BULLISH"
    else:
        direction = "BEARISH"

    return {
        **d,
        "day": str(d["day"]),
        "wds": round(wds, 4),
        "dir_score":   round(dir_score, 4),
        "tier1_ratio": round(tier1_ratio, 4),
        "choppiness_index": round(ci, 4),
        "amp_signed":  round(amp_signed, 4),
        "classification": day_class,
        "direction": direction,
    }


def aggregate_patterns(days: list[dict]) -> dict:
    """
    Agrega padrões recorrentes por:
    - Dia da semana (DOW)
    - Mês do ano
    - Par (DOW x Sessão dominante do dia)
    """
    DOW_NAMES = {0:"Sun", 1:"Mon", 2:"Tue", 3:"Wed", 4:"Thu", 5:"Fri", 6:"Sat"}
    MONTH_NAMES = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                   7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}

    trend_days  = [d for d in days if d["classification"] == "TREND_DAY"]
    noise_days  = [d for d in days if d["classification"] == "NOISE_DAY"]
    mixed_days  = [d for d in days if d["classification"] == "MIXED_DAY"]

    # ── Por dia da semana ─────────────────────────────────────────────
    dow_stats: dict = defaultdict(lambda: {"trend":0,"noise":0,"mixed":0,"wds_sum":0,"count":0})
    for d in days:
        k = DOW_NAMES.get(d["dow"], str(d["dow"]))
        dow_stats[k]["count"]   += 1
        dow_stats[k]["wds_sum"] += d["wds"]
        dow_stats[k][d["classification"].lower().split("_")[0]] += 1

    dow_profile = {}
    for dow, s in dow_stats.items():
        n = max(s["count"], 1)
        dow_profile[dow] = {
            "avg_wds":    round(s["wds_sum"] / n, 4),
            "trend_pct":  round(s["trend"] / n * 100, 1),
            "noise_pct":  round(s["noise"] / n * 100, 1),
            "sample_n":   s["count"],
        }

    # ── Por mês ───────────────────────────────────────────────────────
    month_stats: dict = defaultdict(lambda: {"trend":0,"noise":0,"mixed":0,"wds_sum":0,"count":0})
    for d in days:
        k = MONTH_NAMES.get(d["month"], str(d["month"]))
        month_stats[k]["count"]   += 1
        month_stats[k]["wds_sum"] += d["wds"]
        month_stats[k][d["classification"].lower().split("_")[0]] += 1

    month_profile = {}
    for m, s in month_stats.items():
        n = max(s["count"], 1)
        month_profile[m] = {
            "avg_wds":    round(s["wds_sum"] / n, 4),
            "trend_pct":  round(s["trend"] / n * 100, 1),
            "noise_pct":  round(s["noise"] / n * 100, 1),
            "sample_n":   s["count"],
        }

    # ── Top táticas dos Trend Days ────────────────────────────────────
    from collections import Counter
    trend_sigs = Counter(d["dominant_sig"] for d in trend_days if d["dominant_sig"])
    noise_sigs = Counter(d["dominant_sig"] for d in noise_days if d["dominant_sig"])

    # ── Días de maior absorção institucional ─────────────────────────
    top_whale_days = sorted(trend_days, key=lambda x: x["wds"], reverse=True)[:20]

    return {
        "summary": {
            "total_days":     len(days),
            "trend_days":     len(trend_days),
            "noise_days":     len(noise_days),
            "mixed_days":     len(mixed_days),
            "trend_pct":      round(len(trend_days) / max(len(days),1) * 100, 1),
            "noise_pct":      round(len(noise_days) / max(len(days),1) * 100, 1),
            "avg_wds_overall": round(sum(d["wds"] for d in days) / max(len(days),1), 4),
        },
        "by_day_of_week":  dow_profile,
        "by_month":        month_profile,
        "top_tactics_trend_days":  dict(trend_sigs.most_common(5)),
        "top_tactics_noise_days":  dict(noise_sigs.most_common(5)),
        "top_20_whale_days":       [
            {"day": d["day"], "wds": d["wds"],
             "direction": d["direction"],
             "dominant_sig": d["dominant_sig"],
             "tier1_pct": round(d["tier1_ratio"]*100,1),
             "range_ticks": d["range_ticks"],
             "choppiness": d["choppiness_index"]}
            for d in top_whale_days
        ],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",     default="./data/backtest_8months.db")
    parser.add_argument("--out",    default="./data/variance_report.json")
    parser.add_argument("--symbol", default="6J")
    parser.add_argument("--min-wds-for-calibration", type=float, default=WDS_TREND_THRESHOLD,
                        help="Somente dias acima deste WDS entram no profiler de calibração")
    args = parser.parse_args()

    logger.info("=== variance_profiler.py ===")
    conn = duckdb.connect(args.db, read_only=True)

    logger.info("Calculando métricas diárias...")
    raw_days  = compute_daily_metrics(conn, args.symbol)
    logger.info("  %d dias encontrados.", len(raw_days))

    scored    = [compute_wds(d) for d in raw_days]
    patterns  = aggregate_patterns(scored)
    conn.close()

    # ── Output completo ───────────────────────────────────────────────
    report = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "symbol":        args.symbol,
            "wds_weights":   {"dir": W_DIR, "tier1": W_TIER1, "ci": W_CI, "amp": W_AMP},
            "thresholds":    {"trend": WDS_TREND_THRESHOLD, "noise": WDS_NOISE_THRESHOLD},
        },
        "patterns":   patterns,
        "daily_scores": [
            {k: v for k, v in d.items()
             if k in ("day","dow","month","wds","classification","direction",
                      "tier1_ratio","choppiness_index","dir_score",
                      "dominant_sig","range_ticks","net_cumdelta","total_clusters")}
            for d in scored
        ],
        # Lista de datas aprovadas para calibração — usada pelo recalibrate_clusters.py
        "calibration_days": [
            d["day"] for d in scored
            if d["wds"] >= args.min_wds_for_calibration
        ]
    }

    import os
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info("=== Resultados ===")
    s = patterns["summary"]
    logger.info("  Total dias:   %d", s["total_days"])
    logger.info("  Trend Days:   %d (%.1f%%)", s["trend_days"], s["trend_pct"])
    logger.info("  Noise Days:   %d (%.1f%%)", s["noise_days"], s["noise_pct"])
    logger.info("  WDS médio:    %.4f", s["avg_wds_overall"])
    logger.info("")
    logger.info("  Melhores dias da semana para operar (6J):")
    for dow, p in sorted(patterns["by_day_of_week"].items(),
                         key=lambda x: x[1]["avg_wds"], reverse=True):
        logger.info("    %s: WDS=%.3f | Trend=%.0f%% | n=%d",
                    dow, p["avg_wds"], p["trend_pct"], p["sample_n"])
    logger.info("")
    logger.info("  Top táticas dos Trend Days:")
    for sig, cnt in patterns["top_tactics_trend_days"].items():
        logger.info("    %-28s %d dias", sig, cnt)
    logger.info("")
    logger.info("Relatório salvo em %s", args.out)


if __name__ == "__main__":
    main()
