import duckdb
from config import Config
from queries import build_mfe_mae_query

def run_test():
    cfg = Config()
    # Conecta readonly (nao bate de frente com o lock de escrita se quiser ler, 
    # mas o DuckDB exige fechar as conexoes ativas ou read_only. O try/except ajuda)
    try:
        conn = duckdb.connect(cfg.db_path, read_only=True)
    except Exception as e:
        print(f"Banco ainda esta travado (Lock). Aguarde a ingestao finalizar! Erro: {e}")
        return
    
    query = build_mfe_mae_query(
        start_date='2026-02-01',
        end_date='2026-03-01',
        signature='absorption_passive',
        session='LONDON',
        regime='RANGING'
    )
    
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
