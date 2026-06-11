import duckdb

def run_test():
    # Conecta readonly (nao bate de frente com o lock de escrita se quiser ler, 
    # mas o DuckDB exige fechar as conexoes ativas ou read_only. O try/except ajuda)
    try:
        conn = duckdb.connect('/home/aidea/data_backtest/backtest_8months.db', read_only=True)
    except Exception as e:
        print(f"Banco ainda esta travado (Lock). Aguarde o script finalizar! Erro: {e}")
        return
    
    query = """
    WITH full_clusters AS (
        SELECT *,
            (AVG(price) OVER (
                PARTITION BY symbol 
                ORDER BY timestamp_ns 
                ROWS BETWEEN 2001 PRECEDING AND 1 PRECEDING)
            - AVG(price) OVER (
                PARTITION BY symbol 
                ORDER BY timestamp_ns 
                ROWS BETWEEN 4001 PRECEDING AND 2002 PRECEDING)
            ) AS price_slope_4h
        FROM liquidity_clusters
        WHERE symbol = '6J' 
          -- Filtro estrito para Fevereiro/2026
          AND timestamp >= '2026-02-01' AND timestamp < '2026-03-01'
    ),
    sampled AS (
        SELECT * FROM full_clusters 
        -- Pega todos os clusters de fevereiro sem sample
        -- Ou TABLESAMPLE RESERVOIR para ser analogo ao Q4, 
        -- mas como so queremos fevereiro, vamos analisar TUDO para ter n alto!
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
            CASE WHEN mfe > mae AND mfe > 0 THEN 1 ELSE 0 END AS win,
            CASE WHEN mfe > mae AND mfe > 0 THEN mfe ELSE 0 END AS gross_profit,
            CASE WHEN mae > mfe THEN mae ELSE 0 END AS gross_loss
        FROM mfe_mae_calc
    )
    SELECT 
        COUNT(*) AS total_samples,
        AVG(win) AS win_rate,
        SUM(gross_profit) / NULLIF(SUM(gross_loss), 0) AS profit_factor,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY mfe) AS mfe_p50,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY mfe) AS mfe_p90,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY mfe) AS mfe_p99,
        
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY mae) AS mae_p50,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY mae) AS mae_p90,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY mae) AS mae_p95,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY mae) AS mae_p99
    FROM scored
    WHERE behavior_signature = 'absorption_passive'
      AND session = 'LONDON'
      AND regime = 'RANGING';
    """
    
    print("Processando Fevereiro de 2026 (Analise OOS Integral - Sem Amostragem)...")
    df = conn.execute(query).fetchdf()
    print("========================================")
    print("FEVEREIRO 2026: absorption_passive_RANGING_LONDON")
    print("========================================")
    
    if df.empty or df['total_samples'][0] == 0:
        print("Nenhuma amostra encontrada. O banco ja indexou fevereiro?")
        return

    print(f"Total Samples : {df['total_samples'][0]}")
    print(f"Win Rate      : {df['win_rate'][0]:.2%}")
    pf = df['profit_factor'][0]
    print(f"Profit Factor : {pf:.2f}" if pf is not None else "Profit Factor: Infinity")
    
    print("--- MAE (Derrotas) ---")
    mae_50 = df['mae_p50'][0] / 0.0000005 if df['mae_p50'][0] else 0
    mae_95 = df['mae_p95'][0] / 0.0000005 if df['mae_p95'][0] else 0
    print(f"MAE P50       : {mae_50:.1f} ticks (Meta: <= 2.0 ticks)")
    print(f"MAE P95       : {mae_95:.1f} ticks (Meta: <= 10.0 ticks)")

    print("--- MFE (Vitorias) ---")
    mfe_90 = df['mfe_p90'][0] / 0.0000005 if df['mfe_p90'][0] else 0
    mfe_99 = df['mfe_p99'][0] / 0.0000005 if df['mfe_p99'][0] else 0
    print(f"MFE P90       : {mfe_90:.1f} ticks")
    print(f"MFE P99       : {mfe_99:.1f} ticks")
    print("========================================")

if __name__ == "__main__":
    run_test()
