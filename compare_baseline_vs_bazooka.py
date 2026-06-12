import duckdb

def compare_models(baseline_db: str, bazooka_db: str):
    print("==========================================================")
    print("      LABORATÓRIO QUANTITATIVO: BASELINE vs BAZOOKA (N=10)")
    print("==========================================================")
    
    query = """
    WITH full_clusters AS (
        SELECT * FROM liquidity_clusters
    ),
    excursions AS (
        SELECT
            behavior_signature,
            session,
            outcome,
            confidence,
            (deltamax * 0.0000005) AS mfe_price,
            (ABS(deltamin) * 0.0000005) AS mae_price,
            CASE 
                WHEN outcome = 'WIN' THEN 1 
                WHEN outcome = 'LOSS' THEN 0 
                ELSE NULL 
            END AS win
        FROM full_clusters
        WHERE behavior_signature = 'absorption_passive'
          AND session = 'LONDON'
          AND timestamp >= '2026-01-01'
          AND timestamp <  '2026-02-01'  -- Trava exclusiva de Janeiro para preview
    )
    SELECT
        COUNT(*) as total_samples,
        SUM(win) * 1.0 / NULLIF(COUNT(win), 0) as win_rate,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY mae_price) / 0.0000005 as mae_p50,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY mfe_price) / 0.0000005 as mfe_p90
    FROM excursions;
    """
    
    print("\n[1] Lendo Baseline Cego (ASOF restrito, N=1)...")
    try:
        conn_base = duckdb.connect(baseline_db, read_only=True)
        df_base = conn_base.execute(query).fetchdf()
        conn_base.close()
    except Exception as e:
        print(f"Erro ao ler Baseline: {e}")
        df_base = None

    print("\n[2] Lendo Nova Modelagem Bazooka (ASOF Dinâmico, N=10)...")
    try:
        conn_baz = duckdb.connect(bazooka_db, read_only=True)
        df_baz = conn_baz.execute(query).fetchdf()
        conn_baz.close()
    except Exception as e:
        print(f"Erro no bazooka_db ({bazooka_db}). Tentando ler do arquivo em andamento...")
        try:
            # Fallback para o arquivo ativo
            conn_baz = duckdb.connect("/home/aidea/data_backtest/backtest_8months.db", read_only=True)
            df_baz = conn_baz.execute(query).fetchdf()
            conn_baz.close()
        except Exception as e2:
            print(f"Erro ao ler banco em andamento: {e2}")
            df_baz = None

    if df_base is not None and not df_base.empty:
        base_samples = df_base['total_samples'][0]
        base_wr = df_base['win_rate'][0]
        base_mae = df_base['mae_p50'][0]
        base_mfe = df_base['mfe_p90'][0]
        
        print("\n--- RESULTADOS ---")
        print(f"BASELINE: Samples={base_samples} | WinRate={base_wr:.2%} | MAE P50={base_mae:.1f}t | MFE P90={base_mfe:.1f}t")
    
    if df_baz is not None and not df_baz.empty:
        baz_samples = df_baz['total_samples'][0]
        baz_wr = df_baz['win_rate'][0]
        baz_mae = df_baz['mae_p50'][0]
        baz_mfe = df_baz['mfe_p90'][0]
        print(f"BAZOOKA : Samples={baz_samples} | WinRate={baz_wr:.2%} | MAE P50={baz_mae:.1f}t | MFE P90={baz_mfe:.1f}t")
        
        if df_base is not None and not df_base.empty:
            print("\n--- DELTA TÁTICO ---")
            print(f"Aumento de Amostras (Redução de Cegueira): +{baz_samples - base_samples} sinais")
            print(f"Variação de Win Rate: {((baz_wr - base_wr) * 100):.2f}%")

if __name__ == "__main__":
    # Ajuste os caminhos conforme o ambiente local
    compare_models(
        baseline_db="/home/aidea/data_backtest/backtest_8months_baseline.db",
        bazooka_db="/home/aidea/data_backtest/backtest_8months_bazooka.db"
    )
