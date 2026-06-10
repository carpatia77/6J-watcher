import duckdb
import json
import logging
import time

from adaptive_pattern_engine import AdaptivePatternEngine
from config import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

DB_PATH = "/home/aidea/data_backtest/backtest_8months.db"
PROFILE_PATH = "./data/profile_8months.json"

from datetime import datetime
from models import LiquidityCluster

def main():
    # ...
    logging.info(f"Conectando ao banco de dados: {DB_PATH}")
    # Abrimos o banco em modo de escrita para poder fazer o UPDATE
    conn = duckdb.connect(DB_PATH, read_only=False)
    
    logging.info("Carregando o Cérebro Calibrado (profile_8months.json)...")
    cfg = Config()
    try:
        engine = AdaptivePatternEngine(profile_path=PROFILE_PATH, cfg=cfg)
    except Exception as e:
        logging.error(f"Erro ao carregar o perfil: {e}")
        return

    logging.info("Extraindo clusters de Outubro para reclassificação (apenas a matemática leve)...")
    
    query = """
        SELECT 
            rowid,
            timestamp_ns,
            price,
            total_ask,
            total_bid,
            cumdelta,
            deltamin,
            deltamax,
            session,
            delta_price_ticks
        FROM liquidity_clusters
    """
    rows = conn.execute(query).fetchall()
    
    logging.info(f"{len(rows)} clusters carregados. Iniciando a varredura com o novo cérebro...")
    
    t0 = time.perf_counter()
    updates = []
    
    for r in rows:
        rowid = r[0]
        # We must create a LiquidityCluster to pass to classify()
        # Since timestamp is required to extract the hour, we can mock it from timestamp_ns
        cluster_mock = LiquidityCluster(
            symbol="6J",
            timestamp=datetime.fromtimestamp(r[1] / 1e9),
            price=r[2],
            session=r[8],
            total_ask=r[3],
            total_bid=r[4],
            cumdelta=r[5],
            deltamin=r[6],
            deltamax=r[7],
            delta_price_ticks=r[9]
        )
        
        sig, conf = engine.classify(cluster_mock)
        updates.append((sig.value, conf, rowid))

    logging.info("Aplicando as novas classificações cirúrgicas no DuckDB...")
    
    # Usamos executemany para ser ultrarrápido
    conn.execute("BEGIN TRANSACTION")
    conn.executemany("""
        UPDATE liquidity_clusters 
        SET behavior_signature = ?, confidence = ?
        WHERE rowid = ?
    """, updates)
    conn.execute("COMMIT")
    
    elapsed = time.perf_counter() - t0
    logging.info(f"Reclassificação completa e gravada no banco em {elapsed:.2f}s!")

if __name__ == "__main__":
    main()
