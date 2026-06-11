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
    cfg = Config()
    
    logging.info(f"Conectando ao banco de dados: {cfg.db_path}")
    conn = duckdb.connect(cfg.db_path, read_only=False)
    
    # Extrair todos os meses disponíveis no banco
    logging.info("Mapeando cronologia para Walk-Forward Calibration...")
    months_query = "SELECT DISTINCT date_trunc('month', timestamp) AS m FROM liquidity_clusters ORDER BY m"
    months = [r[0] for r in conn.execute(months_query).fetchall()]
    
    if not months:
        logging.warning("Nenhum dado encontrado para reclassificar.")
        return
        
    logging.info(f"Encontrados {len(months)} meses. Iniciando reclassificação estrita (sem look-ahead bias).")
    
    from signature_profiler import SignatureProfiler
    profiler = SignatureProfiler(cfg.db_path, cfg, conn=conn)
    
    updates = []
    t0 = time.perf_counter()
    
    for month_start in months:
        month_str = month_start.strftime('%Y-%m-%d')
        logging.info(f"\n--- Processando Mês: {month_str} ---")
        
        # 1. Calibrar o cérebro usando APENAS os 30 dias imediatamente anteriores a este mês
        logging.info(f"Calibrando percentis empíricos com base no histórico anterior a {month_str}...")
        try:
            profile = profiler.build_profile(
                symbol=cfg.symbol,
                lookback_days=30,
                horizon_minutes=30, # ou o horizonte desejado
                since=month_str
            )
            
            engine = AdaptivePatternEngine(profile_path="", cfg=cfg)
            if "error" in profile:
                logging.warning(f"Sem dados suficientes antes de {month_str}. Usando perfil Fallback.")
                engine.profile = engine._fallback_profile()
            else:
                logging.info(f"Perfil de calibração criado com sucesso. Aplicando...")
                engine.profile = profile
        except Exception as e:
            logging.error(f"Erro ao gerar perfil para {month_str}: {e}")
            logging.info("Usando perfil Fallback para este mês.")
            engine = AdaptivePatternEngine(profile_path="", cfg=cfg)
            engine.profile = engine._fallback_profile()

        # 2. Extrair dados APENAS deste mês
        query = f"""
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
            WHERE timestamp >= '{month_str}' AND timestamp < CAST('{month_str}' AS TIMESTAMP) + INTERVAL '1' MONTH
        """
        rows = conn.execute(query).fetchall()
        logging.info(f"{len(rows)} clusters carregados para {month_str}.")
        
        # 3. Classificar
        for r in rows:
            rowid = r[0]
            cluster_mock = LiquidityCluster(
                symbol=cfg.symbol,
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

    logging.info(f"\nGravação em lote: aplicando {len(updates)} novas classificações cirúrgicas no DuckDB...")
    
    conn.execute("BEGIN TRANSACTION")
    conn.executemany("""
        UPDATE liquidity_clusters 
        SET behavior_signature = ?, confidence = ?
        WHERE rowid = ?
    """, updates)
    conn.execute("COMMIT")
    
    elapsed = time.perf_counter() - t0
    logging.info(f"Reclassificação Walk-Forward completa com sucesso em {elapsed:.2f}s!")

if __name__ == "__main__":
    main()
