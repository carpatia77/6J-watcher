import duckdb
import time

def main():
    db_path = "data/backtest_8months.db"
    print(f"[*] Conectando ao banco {db_path}...")
    
    try:
        con = duckdb.connect(db_path)
    except Exception as e:
        print(f"[!] Erro ao conectar. Certifique-se de que a extração foi abortada e o arquivo não está travado. Erro: {e}")
        return

    print("[*] Removendo tabelas de dados brutos (tape_events e dom_levels)...")
    
    try:
        con.execute("DROP TABLE IF EXISTS tape_events")
        print("  - Tabela tape_events deletada.")
    except Exception as e:
        print(f"  - Erro ao dropar tape_events: {e}")

    try:
        con.execute("DROP TABLE IF EXISTS dom_levels")
        print("  - Tabela dom_levels deletada.")
    except Exception as e:
        print(f"  - Erro ao dropar dom_levels: {e}")

    print("[*] Iniciando VACUUM para desfragmentar o arquivo e liberar o disco...")
    print("    (Isso pode levar alguns minutos dependendo do SSD)")
    
    t0 = time.time()
    try:
        con.execute("VACUUM")
        elapsed = time.time() - t0
        print(f"[*] VACUUM concluído em {elapsed:.1f} segundos!")
    except Exception as e:
        print(f"[!] Erro no VACUUM: {e}")

    print("[*] Otimização concluída. Seu disco SSD agora deve estar respirando aliviado.")
    con.close()

if __name__ == "__main__":
    main()
