import duckdb
import os
import time

DB_PATH = 'data/backtest_8months.db'

print(f"[*] Conectando ao DuckDB: {DB_PATH}")
con = duckdb.connect(DB_PATH)

# Verifica as contagens
count_tape = con.execute("SELECT count(*) FROM tape_events").fetchone()[0]
count_dom = con.execute("SELECT count(*) FROM dom_levels").fetchone()[0]

print(f"Tape events: {count_tape}, DOM levels: {count_dom}")

if count_tape == 0:
    print("Banco de dados vazio! Precisa rodar o backtest novamente.")
else:
    print("Populando liquidity_clusters a partir dos dados do DuckDB usando a CTE...")
    t0 = time.time()
    
    # Executa a CTE que constrói os clusters DIRETAMENTE no banco de dados!
    # A consulta CTE usada no ingestion.py:
    
    sql = """
    INSERT INTO liquidity_clusters (
        symbol, timestamp, timestamp_ns, price, session, behavior_signature,
        total_ask, total_bid, cumdelta, deltamin, deltamax,
        delta_price_ticks, confidence, outcome, batch_id, raw_payload
    )
    WITH events AS (
        SELECT 
            t.symbol,
            t.timestamp_ns,
            t.price,
            t.volume,
            t.side,
            t.batch_id,
            d.bid_volume,
            d.ask_volume,
            d.price as dom_price,
            d.level_index
        FROM tape_events t
        ASOF JOIN dom_levels d
          ON t.symbol = d.symbol
         AND d.timestamp_ns <= t.timestamp_ns
         AND d.price >= t.price - 0.005
         AND d.price <= t.price + 0.005
    ),
    clusters_raw AS (
        SELECT 
            symbol,
            batch_id,
            timestamp_ns,
            price,
            SUM(CASE WHEN side = 'sell' THEN volume ELSE 0 END) as total_ask,
            SUM(CASE WHEN side = 'buy' THEN volume ELSE 0 END) as total_bid,
            MIN(volume) as min_vol,
            MAX(volume) as max_vol
        FROM events
        GROUP BY symbol, batch_id, timestamp_ns, price
    ),
    cumulative AS (
        SELECT 
            symbol,
            batch_id,
            timestamp_ns,
            price,
            total_ask,
            total_bid,
            total_bid - total_ask as delta_step,
            SUM(total_bid - total_ask) OVER (PARTITION BY symbol ORDER BY timestamp_ns ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumdelta
        FROM clusters_raw
    )
    SELECT 
        symbol,
        to_timestamp(timestamp_ns / 1000000000.0) as timestamp,
        timestamp_ns,
        price,
        'Regular' as session,
        'UNKNOWN' as behavior_signature,
        total_ask,
        total_bid,
        cumdelta,
        MIN(cumdelta) OVER (PARTITION BY symbol ORDER BY timestamp_ns ROWS BETWEEN 50 PRECEDING AND CURRENT ROW) as deltamin,
        MAX(cumdelta) OVER (PARTITION BY symbol ORDER BY timestamp_ns ROWS BETWEEN 50 PRECEDING AND CURRENT ROW) as deltamax,
        0 as delta_price_ticks,
        0.0 as confidence,
        'PENDING' as outcome,
        batch_id,
        '{}' as raw_payload
    FROM cumulative
    """
    
    con.execute("DELETE FROM liquidity_clusters")
    con.execute(sql)
    
    count_clusters = con.execute("SELECT count(*) FROM liquidity_clusters").fetchone()[0]
    print(f"Sucesso! Inseridos {count_clusters} clusters em {time.time() - t0:.2f}s")
