import duckdb
from signature_profiler import SignatureProfiler
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DB_PATH = "/home/aidea/data_backtest/backtest_8months.db"
PROFILE_PATH = "./data/profile_8months.json"

def main():
    logging.info(f"Conectando ao banco de dados: {DB_PATH}")
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    # Habilita a barra de progresso nativa do DuckDB no terminal!
    conn.execute("PRAGMA enable_progress_bar=true")
    
    # Verifica a quantidade de clusters disponíveis
    count = conn.execute("SELECT COUNT(*) FROM liquidity_clusters").fetchone()[0]
    logging.info(f"Total de clusters disponiveis para calibragem: {count}")
    
    if count == 0:
        logging.error("Nenhum cluster encontrado. Abortando profiler.")
        return

    profiler = SignatureProfiler(DB_PATH, conn=conn)
    
    logging.info("Iniciando calibragem MFE/MAE. O DuckDB vai varrer todos os eventos...")
    t0 = time.perf_counter()
    
    try:
        profile = profiler.build_profile(
            symbol="6J", 
            lookback_days=31,  # Pega o mês de Outubro inteiro
            horizon_minutes=5, 
            since="2025-10-31" # A data final dos nossos dados
        )
        
        profiler.save_profile(profile, PROFILE_PATH)
        elapsed = time.perf_counter() - t0
        
        logging.info(f"Calibragem concluida com sucesso em {elapsed:.1f}s!")
        logging.info(f"Arquivo gerado salvo em: {PROFILE_PATH}")
        
    except Exception as e:
        logging.exception("Falha durante o Profiler:")

if __name__ == "__main__":
    main()
