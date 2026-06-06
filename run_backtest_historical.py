from __future__ import annotations
import os
import sys
import logging
from datetime import date

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

CHUNKS = [
    (date(2025, 10, 5), date(2025, 10, 31)),
    (date(2025, 11, 1), date(2025, 11, 30)),
    (date(2025, 12, 1), date(2025, 12, 31)),
    (date(2026, 1, 1),  date(2026, 1, 31)),
    (date(2026, 2, 1),  date(2026, 2, 28)),
    (date(2026, 3, 1),  date(2026, 3, 31)),
    (date(2026, 4, 1),  date(2026, 4, 30)),
    (date(2026, 5, 1),  date(2026, 6, 5)),
]

def main():
    logger.info("=" * 60)
    logger.info("Iniciando orquestrador de backtest historico (8 meses)")
    logger.info(f"Log salvo em: ./data/backtest_run.log")
    logger.info("=" * 60)

    if not API_KEY:
        logger.error("DATABENTO_API_KEY nao definida — abortando.")
        sys.exit(1)

    runner = BacktestRunner(
        api_key=API_KEY,
        db_path="./data/backtest_8months.db",
        profile_path="./data/profile_8months.json",
        batch_size_seconds=60,
        skip_dom=True,
    )

    import time
    for i, (start_dt, end_dt) in enumerate(CHUNKS):
        logger.info("")
        logger.info("=============================================")
        logger.info(f"PROCESSANDO MES {i+1}/8: {start_dt} -> {end_dt}")
        logger.info("=============================================")
        t0 = time.time()
        try:
            runner.run(start=start_dt, end=end_dt, symbol="6J")
        except Exception:
            logger.exception(f"CRASH no mes {start_dt} — traceback completo:")
            logger.error("Abortando backtest para preservar dados ja processados.")
            break

        # CHECKPOINT removido daqui pois o runner já o faz periodicamente e no finally

        elapsed = (time.time() - t0) / 3600
        try:
            count = runner.repo.conn.execute(
                "SELECT COUNT(*), "
                "SUM(CASE WHEN behavior_signature != 'unknown' THEN 1 ELSE 0 END) "
                "FROM liquidity_clusters WHERE symbol='6J' "
                "AND timestamp >= ? AND timestamp < ?",
                [str(start_dt), str(end_dt)]
            ).fetchone()
            total, classified = count[0] or 0, count[1] or 0
            pct = (classified / total * 100) if total > 0 else 0.0
            logger.info(
                f"  Mes {start_dt.strftime('%b/%Y')}: {total:,} clusters, "
                f"{pct:.1f}% classificados, {elapsed:.2f}h"
            )
        except Exception:
            logger.exception("Erro ao gerar relatorio parcial:")

        if os.getenv("BACKTEST_INTERACTIVE", "0") == "1":
            input(f"\n[PAUSA] Mes {i+1} concluido. Pressione Enter para continuar...")

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
