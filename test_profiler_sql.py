import duckdb
from datetime import datetime, timedelta

def test_sql():
    conn = duckdb.connect(':memory:')
    
    # Setup dummy tables
    conn.execute("""
    CREATE TABLE liquidity_clusters (
        symbol VARCHAR, timestamp TIMESTAMP, price DOUBLE, session VARCHAR,
        behavior_signature VARCHAR, total_ask INTEGER, total_bid INTEGER, delta_price_ticks INTEGER
    )
    """)
    conn.execute("""
    CREATE TABLE tape_events (
        symbol VARCHAR, timestamp TIMESTAMP, price DOUBLE
    )
    """)
    
    # Insert dummy data
    conn.execute("INSERT INTO liquidity_clusters VALUES ('6J', '2023-01-01 10:00:00', 100.0, 'NEW_YORK', 'iceberg_accumulation', 10, 50, 0)")
    conn.execute("INSERT INTO tape_events VALUES ('6J', '2023-01-01 10:01:00', 105.0)")
    conn.execute("INSERT INTO tape_events VALUES ('6J', '2023-01-01 10:05:00', 95.0)")

    symbol = '6J'
    cutoff = '2022-01-01 00:00:00'
    interval_clause = "INTERVAL '30' MINUTE"
    
    base_query = f"""
    WITH cluster_excursions AS (
        SELECT 
            c.timestamp,
            c.price AS c_price,
            c.delta_price_ticks,
            c.behavior_signature,
            c.session,
            c.total_bid,
            c.total_ask,
            (c.total_bid + c.total_ask) AS total_vol,
            ABS(c.total_bid - c.total_ask) AS imbalance,
            COALESCE(MAX(t.price) OVER (
                PARTITION BY c.symbol 
                ORDER BY t.timestamp 
                RANGE BETWEEN CURRENT ROW AND {interval_clause} FOLLOWING
            ), c.price) AS max_future_price,
            COALESCE(MIN(t.price) OVER (
                PARTITION BY c.symbol 
                ORDER BY t.timestamp 
                RANGE BETWEEN CURRENT ROW AND {interval_clause} FOLLOWING
            ), c.price) AS min_future_price
        FROM liquidity_clusters c
        LEFT JOIN tape_events t 
          ON c.symbol = t.symbol 
         AND t.timestamp >= c.timestamp 
         AND t.timestamp <= c.timestamp + {interval_clause}
        WHERE c.symbol = ? AND c.timestamp > ?
    ),
    mfe_mae_calc AS (
        SELECT 
            *,
            CASE WHEN behavior_signature IN ('iceberg_accumulation', 'breakout_genuine', 'magnet_effect') THEN TRUE ELSE FALSE END AS is_bullish,
            CASE WHEN behavior_signature IN ('iceberg_accumulation', 'breakout_genuine', 'magnet_effect') 
                 THEN max_future_price - c_price 
                 ELSE c_price - min_future_price END AS mfe,
            CASE WHEN behavior_signature IN ('iceberg_accumulation', 'breakout_genuine', 'magnet_effect') 
                 THEN c_price - min_future_price 
                 ELSE max_future_price - c_price END AS mae
        FROM cluster_excursions
    )
    SELECT 
        *,
        CASE WHEN mfe > ABS(mae) AND mfe > 0 THEN 1 ELSE 0 END AS win,
        CASE WHEN mfe > 0 THEN mfe ELSE 0 END AS gross_profit,
        CASE WHEN mae < 0 THEN ABS(mae) ELSE 0 END AS gross_loss
    FROM mfe_mae_calc
    """
    
    rel = conn.execute(base_query, [symbol, cutoff])
    print("BASE QUERY RESULTS:")
    print(rel.fetchdf())
    
    sig_query = """
        SELECT 
            behavior_signature,
            session,
            COUNT(*) AS count,
            SUM(win) AS wins,
            SUM(gross_profit) AS total_gross_profit,
            SUM(gross_loss) AS total_gross_loss,
            AVG(mfe) AS avg_mfe
        FROM rel
        GROUP BY behavior_signature, session
    """
    print("SIG QUERY RESULTS:")
    print(conn.sql(sig_query).fetchdf())
    
    perc_query = """
        SELECT 
            session,
            COUNT(*) AS session_count,
            QUANTILE_CONT(total_vol, 0.50) AS vol_p50,
            QUANTILE_CONT(total_vol, 0.75) AS vol_p75,
            QUANTILE_CONT(total_vol, 0.90) AS vol_p90,
            QUANTILE_CONT(total_vol, 0.95) AS vol_p95,
            QUANTILE_CONT(total_vol, 0.99) AS vol_p99,
            QUANTILE_CONT(imbalance, 0.50) AS imb_p50,
            QUANTILE_CONT(imbalance, 0.75) AS imb_p75,
            QUANTILE_CONT(imbalance, 0.90) AS imb_p90,
            QUANTILE_CONT(imbalance, 0.95) AS imb_p95,
            QUANTILE_CONT(imbalance, 0.99) AS imb_p99
        FROM rel
        GROUP BY session
    """
    print("PERC QUERY RESULTS:")
    print(conn.sql(perc_query).fetchdf())

if __name__ == "__main__":
    test_sql()
