from __future__ import annotations
import os
import sys
import logging
from datetime import date, timedelta

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

def _setup_logging():
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    sh.terminator = "\n"
    os.makedirs("./data", exist_ok=True)
    fh = logging.FileHandler("./data/backtest_run.log", mode="a", encoding="utf-8")
    fh.setFormatter(fmt)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    if not root.handlers:
        root.addHandler(sh)
        root.addHandler(fh)

_setup_logging()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backtest.backtest_runner import BacktestRunner

logger = logging.getLogger(__name__)

API_KEY = os.getenv("DATABENTO_API_KEY", "")

# Cada chunk = 1 arquivo .dbn.zst independente no cache.
# Meses separados permitem reprocessamento granular sem re-download.
CHUNKS = [
    # (date(2025, 10, 5),   date(2025, 10, 31)), # Outubro IS processado
    (date(2025, 11, 1),   date(2025, 11, 30)), # Novembro IS
    (date(2025, 12, 1),   date(2025, 12, 31)), # Dezembro IS
]

TOTAL_CHUNKS = len(CHUNKS)


def _print_chunk1_decision_panel(runner: BacktestRunner):
    """
    Após o primeiro chunk, imprime um painel de decisão no stdout
    para que o operador possa decidir se aborta e implementa a
    vetorização antes de continuar os chunks restantes.
    """
    prof = runner.phase_profiler
    if prof is None:
        return

    phases = prof.phase_totals()
    wall   = prof.total_wall()
    ingest_pct = (phases.get("ingest", 0) / (sum(phases.values()) or 1)) * 100
    proj_total_h = (wall / 3600) * TOTAL_CHUNKS
    proj_remain_h = (wall / 3600) * (TOTAL_CHUNKS - 1)

    lines = [
        "",
        "*" * 62,
        "  PAINEL DE DECIS\u00c3O — CHUNK 1 CONCLU\u00cdDO",
        "*" * 62,
        f"  Chunk 1 (Oct/2025):  {wall/3600:.2f}h",
        f"  Proje\u00e7\u00e3o total (x{TOTAL_CHUNKS}): {proj_total_h:.1f}h",
        f"  Proje\u00e7\u00e3o restante:     {proj_remain_h:.1f}h",
        "",
        f"  HOT-PATH (ingest loop):  {ingest_pct:.1f}% do tempo",
        "",
    ]

    if ingest_pct >= 60.0:
        lines += [
            "  *** RECOMENDACAO: PARAR E VECTORIZAR ***",
            "",
            "  O loop Python domina >60% do tempo.",
            f"  Vetoriza\u00e7\u00e3o estimada: reduz {proj_total_h:.1f}h -> ",
            f"  ~{proj_total_h * 0.25:.1f}h (estimativa conservadora -75%).",
            "",
            "  ACAO: Ctrl+C agora. Abra a issue de vetoriza\u00e7\u00e3o.",
            "        O chunk 1 ja esta no DB — nao ha perda de dados.",
        ]
    elif ingest_pct >= 35.0:
        lines += [
            "  >> AVALIACAO: ganho moderado com vetoriza\u00e7\u00e3o.",
            f"  Economia estimada: ~{proj_total_h * 0.4:.1f}h (-40%).",
            "",
            "  ACAO: decida com base na disponibilidade de tempo.",
            "        Pressione Enter para continuar ou Ctrl+C para parar.",
        ]
    else:
        lines += [
            "  OK: HOT-PATH < 35%. Bottleneck e I/O ou SQL.",
            "  Vetoriza\u00e7\u00e3o Python teria impacto baixo.",
            "",
            "  ACAO: continuar normalmente.",
        ]

    lines += [
        "",
        "  Para continuar: Enter (ou BACKTEST_INTERACTIVE=0)",
        "*" * 62,
        "",
    ]

    panel = "\n".join(lines)
    # Imprime no stdout E no log
    print(panel, flush=True)
    logger.info(panel)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run historical backtest")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--skip-profiler", action="store_true", help="Skip signature profiler generation")
    parser.add_argument("--skip-download", action="store_true", help="Do not attempt to download files, only read from cache")
    parser.add_argument("--cache-dir", type=str, default="/home/aidea/data_backtest/databento", help="Path to databento cache directory")
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Iniciando orquestrador de backtest historico")
    logger.info(f"Log salvo em: ./data/backtest_run.log")
    logger.info("=" * 60)

    if not API_KEY:
        logger.error("DATABENTO_API_KEY nao definida - abortando.")
        sys.exit(1)

    native_db_dir = "/home/aidea/data_backtest"
    import os
    if not os.path.exists(native_db_dir):
        os.makedirs(native_db_dir, exist_ok=True)
        
    native_db_path = f"{native_db_dir}/backtest_2025_train.db"
    
    # REMOVIDO: A delecao incondicional do banco destruiu os 650M de linhas da run anterior!
    # O DuckDB fara o append/upsert normalmente.

    runner = BacktestRunner(
        api_key=API_KEY,
        db_path=native_db_path,
        profile_path="./data/profile_8months.json",
        batch_size_seconds=60,
        skip_dom=False,
        skip_profiler=args.skip_profiler,
        cache_dir=args.cache_dir,
    )

    import time
    
    if args.start and args.end:
        start_date = date.fromisoformat(args.start)
        end_date = date.fromisoformat(args.end)
        chunks_to_run = [(start_date, end_date)]
    else:
        chunks_to_run = CHUNKS

    total_chunks = len(chunks_to_run)

    for i, (start_dt, end_dt) in enumerate(chunks_to_run):
        logger.info("")
        logger.info("=============================================")
        logger.info(f"PROCESSANDO CHUNK {i+1}/{total_chunks}: {start_dt} -> {end_dt}")
        logger.info("=============================================")
        t0 = time.time()

        runner._last_market_ts = None

        try:
            runner.run(
                start=start_dt,
                end=end_dt,
                symbol="6J",
                skip_download=args.skip_download,
                total_chunks=total_chunks,   # para projeção correta no profiler
            )
        except Exception:
            logger.exception(f"CRASH no chunk {start_dt} - traceback completo:")
            logger.error("Abortando backtest para preservar dados ja processados.")
            break

        # Prune da matriz na transição entre meses
        if runner._last_market_ts:
            runner.matrix.prune_stale_data(hours=0, reference_time=runner._last_market_ts)
            logger.info("[prune] Matriz zerada apos chunk %s", end_dt)

        elapsed = (time.time() - t0) / 3600

        try:
            count = runner.repo.conn.execute(
                "SELECT COUNT(*), "
                "SUM(CASE WHEN behavior_signature != 'unknown' THEN 1 ELSE 0 END) "
                "FROM liquidity_clusters WHERE symbol='6J' "
                "AND timestamp >= ? AND timestamp < ?",
                [str(start_dt), str(end_dt + timedelta(days=1))]
            ).fetchone()
            total, classified = count[0] or 0, count[1] or 0
            pct = (classified / total * 100) if total > 0 else 0.0
            logger.info(
                f"  Chunk {start_dt.strftime('%b/%Y')}: {total:,} clusters, "
                f"{pct:.1f}% classificados, {elapsed:.2f}h"
            )
        except Exception:
            logger.exception("Erro ao gerar relatorio parcial:")

        # Painel de decisão após chunk 1
        if i == 0 and total_chunks > 1:
            _print_chunk1_decision_panel(runner)
            # Pausa interativa apenas se BACKTEST_INTERACTIVE=1
            if os.getenv("BACKTEST_INTERACTIVE", "0") == "1":
                try:
                    input("\n[PAUSA] Chunk 1 concluido. Pressione Enter para continuar "
                          "(Ctrl+C para abortar e vectorizar)...")
                except KeyboardInterrupt:
                    logger.info("[PAUSA] Operador abortou apos chunk 1. Dados preservados.")
                    sys.exit(0)
        elif os.getenv("BACKTEST_INTERACTIVE", "0") == "1":
            try:
                input(f"\n[PAUSA] Chunk {i+1} concluido. Pressione Enter para continuar...")
            except KeyboardInterrupt:
                logger.info("[PAUSA] Operador abortou no chunk %d. Dados preservados.", i+1)
                sys.exit(0)

    logger.info("Salvando relatorio consolidado...")
    try:
        runner.save_report("./data/backtest_8months_report.md")
    except Exception:
        logger.exception("Erro ao salvar relatorio final:")
    finally:
        try:
            runner.repo.close()
        except Exception:
            pass
        logger.info("Conexao DuckDB encerrada.")

    logger.info("Orquestrador concluido. Veja ./data/backtest_run.log para historico completo.")

if __name__ == "__main__":
    main()
