from __future__ import annotations
import os
import sys
import logging
from datetime import date

# Garante import do modulo backtest
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backtest.backtest_runner import BacktestRunner

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

API_KEY = "db-RiQAXJTPv69L4fTkWLNVut7NyjhjX"

# O usuario pediu de 2025-10-05 ate 2026-06-05 (8 meses)
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
    logger.info("Iniciando orquestrador de backtest historico (8 meses)")
    runner = BacktestRunner(
        api_key=API_KEY,
        db_path="./data/backtest_8months.db",
        profile_path="./data/profile_8months.json",
        batch_size_seconds=60
    )

    for start_dt, end_dt in CHUNKS:
        logger.info(f"\n=============================================")
        logger.info(f"PROCESSANDO CHUNK: {start_dt} a {end_dt}")
        logger.info(f"=============================================")
        try:
            runner.run(start=start_dt, end=end_dt, symbol="6J")
            runner.repo.conn.execute("CHECKPOINT")
            logger.info(f"Checkpoint concluído para {start_dt} a {end_dt}")
        except Exception as e:
            logger.error(f"Erro ao processar chunk {start_dt} a {end_dt}: {e}")
            # continua proximo chunk em vez de abortar tudo
            continue

    logger.info("Salvando relatorio consolidado...")
    runner.save_report("./data/backtest_8months_report.md")
    logger.info("Orquestrador concluido.")

if __name__ == "__main__":
    main()
