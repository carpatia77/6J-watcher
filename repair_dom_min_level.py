import duckdb

def repair():
    db_path = "data/backtest_8months.db"
    print("Conectando ao banco de dados...")
    conn = duckdb.connect(db_path)
    
    print("Extraindo dom_min_level do raw_payload para a coluna nativa...")
    res = conn.execute("""
        UPDATE liquidity_clusters 
        SET dom_min_level = CAST(json_extract_string(raw_payload, '$.dom_min_level') AS INTEGER)
        WHERE raw_payload LIKE '%"dom_min_level"%'
    """)
    
    print("Atualização concluída.")
    
    print("Verificando valores atualizados...")
    counts = conn.execute("""
        SELECT dom_min_level, COUNT(*) 
        FROM liquidity_clusters 
        GROUP BY dom_min_level 
        ORDER BY dom_min_level
    """).fetchall()
    
    for row in counts:
        print(f"Level {row[0]}: {row[1]} clusters")
        
    conn.close()

if __name__ == "__main__":
    repair()
