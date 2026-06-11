import duckdb

from config import Config
from queries import build_mfe_mae_query

def run_test():
    cfg = Config()
    try:
        conn = duckdb.connect(cfg.db_path, read_only=True)
    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return
    
    query = build_mfe_mae_query(
        start_date='2026-04-01',
        end_date='2026-05-01',
        signature='absorption_passive',
        session='LONDON',
        regime='RANGING'
    )
    
    print("Processando Abril de 2026 (Analise OOS Integral - Sem Amostragem)...")
    df = conn.execute(query).fetchdf()
    print("========================================")
    print("ABRIL 2026: absorption_passive_RANGING_LONDON")
    print("========================================")
    
    if df.empty or df['total_samples'][0] == 0:
        print("Nenhuma amostra encontrada.")
        return

    print(f"Total Samples : {df['total_samples'][0]}")
    print(f"Win Rate      : {df['win_rate'][0]:.2%}")
    pf = df['profit_factor'][0]
    print(f"Profit Factor : {pf:.2f}" if pf is not None else "Profit Factor: Infinity")
    print(f" -> META: > 3.00 [{'OK' if pf and pf > 3.0 else 'FAIL'}]")
    
    print("--- MAE (Derrotas - Risco Estrutural) ---")
    mae_50 = df['mae_p50'][0] / 0.0000005 if df['mae_p50'][0] else 0
    mae_95 = df['mae_p95'][0] / 0.0000005 if df['mae_p95'][0] else 0
    print(f"MAE P50       : {mae_50:.1f} ticks (Meta: <= 2.0 ticks) [{'OK' if mae_50 <= 2.0 else 'FAIL'}]")
    print(f"MAE P95       : {mae_95:.1f} ticks (Meta: <= 10.0 ticks) [{'OK' if mae_95 <= 10.0 else 'FAIL'}]")

    print("--- MFE (Vitorias - Event-Driven) ---")
    mfe_90 = df['mfe_p90'][0] / 0.0000005 if df['mfe_p90'][0] else 0
    mfe_99 = df['mfe_p99'][0] / 0.0000005 if df['mfe_p99'][0] else 0
    print(f"MFE P90       : {mfe_90:.1f} ticks (Meta: > 10.0 ticks) [{'OK' if mfe_90 > 10.0 else 'FAIL'}]")
    print(f"MFE P99       : {mfe_99:.1f} ticks (Meta: > 30.0 ticks) [{'OK' if mfe_99 > 30.0 else 'FAIL'}]")
    print("========================================")

if __name__ == "__main__":
    run_test()
