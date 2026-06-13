import duckdb
import os
import sys

def main():
    db_path = "/home/aidea/data_backtest/backtest_2025_train.db"
    if not os.path.exists(db_path):
        print(f"Banco nao encontrado: {db_path}")
        sys.exit(1)

    conn = duckdb.connect(db_path, read_only=True)
    
    query = """
    SELECT 
        price_level,
        COUNT(*) as total_cancels,
        AVG(size) as avg_size,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY size) as p50_size,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY size) as p90_size,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY size) as p99_size
    FROM cancel_events
    WHERE snapshots_present >= 3 
    GROUP BY price_level
    ORDER BY price_level
    LIMIT 20;
    """
    
    print("Executando calibração de níveis...")
    df = conn.execute(query).df()
    
    print("Resultados de calibração (todos os lados):")
    print(df.to_string(index=False))
    
    out_path = "/mnt/c/Users/aidea/.gemini/6J-watcher/data/calibration_results.md"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("# Calibração de Níveis - Cancelamentos (Outubro 2025)\n\n")
        f.write(df.to_string(index=False))
        
    conn.close()
    print("\nSalvo em data/calibration_results.md")

if __name__ == "__main__":
    main()
