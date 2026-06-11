import duckdb

def run_test():
    conn = duckdb.connect('/home/aidea/data_backtest/backtest_8months.db', read_only=True)
    
    query = """
    WITH full_clusters AS (
        SELECT *,
            (AVG(price) OVER (
                PARTITION BY symbol 
                ORDER BY timestamp_ns 
                ROWS BETWEEN 2000 PRECEDING AND CURRENT ROW)
            - AVG(price) OVER (
                PARTITION BY symbol 
                ORDER BY timestamp_ns 
                ROWS BETWEEN 4000 PRECEDING AND 2001 PRECEDING)
            ) AS price_slope_4h
        FROM liquidity_clusters
        WHERE symbol = '6J' AND timestamp > '2025-10-01'
    ),
    sampled AS (
        SELECT * FROM full_clusters 
        -- Nao vamos usar TABLESAMPLE aqui para termos visao completa de todos os eventos 
        -- ou podemos usar TABLESAMPLE igual ao profiler para ver a densidade do profiler
        TABLESAMPLE RESERVOIR(20000 ROWS)
    ),
    cluster_excursions AS (
        SELECT
            c.timestamp,
            c.behavior_signature,
            c.session,
            c.price AS c_price,
            c.total_bid,
            c.total_ask,
            COALESCE(MAX(t.price), c.price)        AS max_future_price,
            COALESCE(MIN(t.price), c.price)        AS min_future_price,
            CASE 
                WHEN ABS(c.price_slope_4h) > 0.000010 THEN 'TRENDING'
                ELSE 'RANGING'
            END AS regime
        FROM sampled c
        LEFT JOIN tape_events t
            ON  c.symbol = t.symbol
            AND (
                CASE WHEN c.timestamp_ns IS NOT NULL AND t.timestamp_ns IS NOT NULL
                     THEN t.timestamp_ns > c.timestamp_ns
                          AND t.timestamp_ns <= c.timestamp_ns + (CASE WHEN c.behavior_signature = 'spoofing_wall' THEN 120000000000 ELSE 120000000000 END)
                     ELSE t.timestamp > c.timestamp
                          AND t.timestamp <= c.timestamp + INTERVAL 1 MINUTE * (CASE WHEN c.behavior_signature = 'spoofing_wall' THEN 2 ELSE 2 END)
                END
            )
        GROUP BY c.timestamp, c.behavior_signature, c.session, c.price, c.price_slope_4h, c.total_bid, c.total_ask
    ),
    mfe_mae_calc AS (
        SELECT *,
            CASE
                WHEN behavior_signature IN ('iceberg_accumulation', 'breakout_genuine', 'defense_line') THEN max_future_price - c_price
                WHEN behavior_signature IN ('iceberg_distribution', 'absorption_passive') THEN c_price - min_future_price
                WHEN behavior_signature IN ('spoofing_wall', 'liquidity_vacuum') AND total_bid > total_ask THEN max_future_price - c_price
                WHEN behavior_signature IN ('spoofing_wall', 'liquidity_vacuum') AND total_ask > total_bid THEN c_price - min_future_price
                ELSE GREATEST(max_future_price - c_price, c_price - min_future_price)
            END AS mfe,
            CASE
                WHEN behavior_signature IN ('iceberg_accumulation', 'breakout_genuine', 'defense_line') THEN c_price - min_future_price
                WHEN behavior_signature IN ('iceberg_distribution', 'absorption_passive') THEN max_future_price - c_price
                WHEN behavior_signature IN ('spoofing_wall', 'liquidity_vacuum') AND total_bid > total_ask THEN c_price - min_future_price
                WHEN behavior_signature IN ('spoofing_wall', 'liquidity_vacuum') AND total_ask > total_bid THEN max_future_price - c_price
                ELSE GREATEST(max_future_price - c_price, c_price - min_future_price)
            END AS mae
        FROM cluster_excursions
    ),
    scored AS (
        SELECT *,
            CASE WHEN mfe > mae AND mfe > 0 THEN 1 ELSE 0 END AS win
        FROM mfe_mae_calc
    )
    SELECT 
        DATE_TRUNC('week', timestamp) AS week,
        COUNT(*) AS n_samples,
        AVG(win) AS win_rate_local
    FROM scored
    WHERE behavior_signature = 'absorption_passive'
      AND session = 'LONDON'
      AND regime = 'RANGING'
    GROUP BY 1
    ORDER BY 1;
    """
    
    df = conn.execute(query).fetchdf()
    print("========================================")
    print("DISTRIBUICAO TEMPORAL: absorption_passive_RANGING_LONDON")
    print("========================================")
    print(df.to_string(index=False))
    print("========================================")
    print(f"Total samples: {df['n_samples'].sum()}")

if __name__ == "__main__":
    run_test()
