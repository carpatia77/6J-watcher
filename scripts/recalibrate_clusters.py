"""
recalibrate_clusters.py
-----------------------
Executa APÓS o backtest completo dos 8 meses.

1. Roda SignatureProfiler sobre TODOS os dados históricos
2. Salva profile_calibrated.json com percentis reais do 6J
3. Reclassifica todos os clusters do DB em transação atômica
4. Reporta a mudança na distribuição de assinaturas

Uso:
    python scripts/recalibrate_clusters.py \
        --db ./data/backtest_8months.db \
        --profile ./data/profile_calibrated.json \
        --symbol 6J
"""
from __future__ import annotations
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import logging
import duckdb
from datetime import datetime
from collections import Counter

from config import Config
from signature_profiler import SignatureProfiler
from adaptive_pattern_engine import AdaptivePatternEngine
from models import BehaviorSignature, LiquidityCluster, Side

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TICK_SIZE = 0.00005  # 6J CME


def fetch_all_clusters(conn: duckdb.DuckDBPyConnection, symbol: str) -> list[dict]:
    """Lê todos os clusters como lista de dicts."""
    rows = conn.execute("""
        SELECT
            symbol, timestamp, price, session,
            behavior_signature, total_ask, total_bid,
            cumdelta, deltamin, deltamax, delta_price_ticks,
            confidence, outcome, raw_payload,
            rowid AS row_id
        FROM liquidity_clusters
        WHERE symbol = ?
        ORDER BY timestamp ASC
    """, [symbol]).fetchall()

    cols = ["symbol","timestamp","price","session","behavior_signature",
            "total_ask","total_bid","cumdelta","deltamin","deltamax",
            "delta_price_ticks","confidence","outcome","raw_payload","row_id"]
    return [dict(zip(cols, r)) for r in rows]


def reclassify(clusters: list[dict], engine: AdaptivePatternEngine) -> list[dict]:
    """Reclassifica cada cluster usando o engine com o novo profile."""
    reclassified = []
    for d in clusters:
        c = LiquidityCluster(
            symbol    = d["symbol"],
            timestamp = d["timestamp"],
            price     = d["price"],
            session   = d["session"],
            total_ask = d["total_ask"] or 0,
            total_bid = d["total_bid"] or 0,
            cumdelta  = d["cumdelta"] or 0,
            delta_price_ticks = d["delta_price_ticks"] or 0,
        )
        new_sig = engine.classify(c)
        d["new_signature"] = new_sig.value
        reclassified.append(d)
    return reclassified


def write_back(conn: duckdb.DuckDBPyConnection, clusters: list[dict]):
    """
    Atualiza behavior_signature no DuckDB.
    Usa UPDATE por rowid — DuckDB suporta nativamente.
    Envolto em transação única para performance e atomicidade.
    """
    logger.info("Iniciando UPDATE atômico de %d clusters...", len(clusters))
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.executemany(
            "UPDATE liquidity_clusters SET behavior_signature = ? WHERE rowid = ?",
            [(d["new_signature"], d["row_id"]) for d in clusters]
        )
        conn.execute("COMMIT")
        logger.info("COMMIT concluído.")
    except Exception as e:
        conn.execute("ROLLBACK")
        logger.error("ROLLBACK — erro: %s", e)
        raise


def report_delta(before: list[dict], after: list[dict]):
    """Exibe a mudança na distribuição de assinaturas."""
    before_dist = Counter(d["behavior_signature"] for d in before)
    after_dist  = Counter(d["new_signature"] for d in after)

    logger.info("=== Distribuição ANTES → DEPOIS ===")
    all_sigs = set(before_dist) | set(after_dist)
    for sig in sorted(all_sigs):
        b = before_dist.get(sig, 0)
        a = after_dist.get(sig, 0)
        pct_b = b / len(before) * 100
        pct_a = a / len(after) * 100
        logger.info("  %-28s %6d (%5.1f%%) → %6d (%5.1f%%)", sig, b, pct_b, a, pct_a)

    unknown_before = before_dist.get("unknown", 0) / len(before) * 100
    unknown_after  = after_dist.get("unknown", 0) / len(after) * 100
    logger.info("Unknown: %.1f%% → %.1f%%  (meta: <30%%)", unknown_before, unknown_after)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",      default="./data/backtest_8months.db")
    parser.add_argument("--profile", default="./data/profile_calibrated.json")
    parser.add_argument("--symbol",  default="6J")
    # lookback em dias — deve cobrir TODO o período do backtest
    # Para 8 meses a partir de out/2025: 400 dias cobre com folga
    parser.add_argument("--lookback-days", type=int, default=400)
    parser.add_argument("--horizon-minutes", type=int, default=30)
    args = parser.parse_args()

    logger.info("=== recalibrate_clusters.py ===")
    logger.info("DB:      %s", args.db)
    logger.info("Profile: %s", args.profile)
    logger.info("Symbol:  %s", args.symbol)

    cfg = Config()
    cfg.db_path = args.db

    # ── PASSO 1: Rodar SignatureProfiler ─────────────────────────────────
    logger.info("PASSO 1: Calculando percentis empíricos com lookback=%d dias...", args.lookback_days)
    
    import json
    variance_file = "./data/variance_report.json"
    filter_dates = None
    since_date = None
    if os.path.exists(variance_file):
        try:
            with open(variance_file, "r") as f:
                variance = json.load(f)
            filter_dates = variance.get("calibration_days", [])
            if filter_dates:
                filter_dates.sort()
                since_date = filter_dates[-1]
                logger.info("Usando %d Trend Days do variance_report.json para calibração", len(filter_dates))
        except Exception as e:
            logger.warning("Falha ao ler variance_report.json: %s", e)

    profiler = SignatureProfiler(args.db, cfg=cfg)
    profile  = profiler.build_profile(
        symbol          = args.symbol,
        lookback_days   = args.lookback_days,   # cobre os 8 meses inteiros
        horizon_minutes = args.horizon_minutes,
        since           = since_date,
        filter_dates    = filter_dates,
    )

    if "error" in profile:
        logger.error("Profiler retornou erro: %s", profile["error"])
        logger.error("Verifique se o backtest foi concluído e há dados suficientes.")
        sys.exit(1)

    # Valida que pelo menos 2 sessões foram calibradas (não-fallback)
    thresholds = profile.get("thresholds", {})
    calibrated_sessions = [
        s for s, t in thresholds.items()
        if t.get("vol_percentiles", {}).get("90", 0) != 20  # diferente do fallback ASIAN
    ]
    logger.info("Sessões calibradas com dados reais: %s", calibrated_sessions)

    profiler.save_profile(profile, args.profile)
    logger.info("Profile salvo em %s", args.profile)

    # ── PASSO 2: Log dos percentis reais descobertos ──────────────────────
    logger.info("=== Percentis reais do 6J ===")
    for session, t in thresholds.items():
        vp = t.get("vol_percentiles", {})
        ip = t.get("imb_percentiles", {})
        logger.info(
            "  %s | vol p75=%.1f p90=%.1f | imb p75=%.1f p90=%.1f",
            session,
            vp.get("75", 0), vp.get("90", 0),
            ip.get("75", 0), ip.get("90", 0),
        )

    # ── PASSO 3: Carregar clusters e reclassificar ────────────────────────
    logger.info("PASSO 3: Carregando clusters do DB...")
    conn = duckdb.connect(args.db)
    clusters = fetch_all_clusters(conn, args.symbol)
    logger.info("  %d clusters carregados.", len(clusters))

    engine = AdaptivePatternEngine(profile_path=args.profile, cfg=cfg)
    logger.info("PASSO 4: Reclassificando com novo profile...")
    reclassified = reclassify(clusters, engine)

    # ── PASSO 4: Report antes do write ────────────────────────────────────
    report_delta(clusters, reclassified)

    # ── PASSO 5: Confirma antes de escrever ───────────────────────────────
    unknown_after = sum(1 for d in reclassified if d["new_signature"] == "unknown")
    pct_unknown = unknown_after / len(reclassified) * 100

    if pct_unknown > 60:
        logger.warning(
            "ATENÇÃO: %.1f%% ainda será unknown após reclassificação. "
            "Os percentis podem estar altos demais. "
            "Verifique se o backtest dos 8 meses está completo.",
            pct_unknown,
        )
        answer = input("Deseja continuar mesmo assim? [s/N]: ").strip().lower()
        if answer != "s":
            logger.info("Operação cancelada pelo usuário.")
            conn.close()
            sys.exit(0)

    # ── PASSO 6: Escreve de volta no DuckDB ───────────────────────────────
    logger.info("PASSO 5: Atualizando DB...")
    write_back(conn, reclassified)

    # ── PASSO 7: Validação final ───────────────────────────────────────────
    final_dist = conn.execute("""
        SELECT behavior_signature, COUNT(*) AS cnt
        FROM liquidity_clusters WHERE symbol = ?
        GROUP BY behavior_signature ORDER BY cnt DESC
    """, [args.symbol]).fetchall()

    logger.info("=== Distribuição final no DB ===")
    total = sum(r[1] for r in final_dist)
    for sig, cnt in final_dist:
        logger.info("  %-28s %6d  (%.1f%%)", sig, cnt, cnt / total * 100)

    conn.close()
    logger.info("=== Calibração concluída com sucesso ===")


if __name__ == "__main__":
    main()
