import sys
import glob
from pathlib import Path
from datetime import datetime
from backtest.backtest_runner import BacktestRunner

def main():
    API_KEY = "dummy_key"
    native_db_path = "/home/aidea/data_backtest/backtest_8months.db"
    
    files = glob.glob("/home/aidea/6j-watcher/data/databento/01-04/*.dbn.zst")
    if not files:
        print("Nenhum arquivo encontrado em /home/aidea/6j-watcher/data/databento/01-04/")
        return
        
    print(f"Encontrados {len(files)} arquivos para ingestão.")
    
    runner = BacktestRunner(
        api_key=API_KEY,
        db_path=native_db_path,
        profile_path="./data/profile_8months.json",
        batch_size_seconds=60,
        skip_dom=False,
        skip_profiler=True, # Vamos rodar no final!
    )
    
    for f in sorted(files):
        # Extrai a data do arquivo (ex: glbx-mdp3-20251001.mbp-10.dbn.zst)
        filename = Path(f).name
        date_str = filename.split('-')[2].split('.')[0]
        dt = datetime.strptime(date_str, "%Y%m%d").date()
        
        print(f"\\n--- Ingerindo arquivo: {filename} (Data: {dt}) ---")
        runner.run(
            start=dt,
            end=dt,
            symbol="6J",
            file_path_override=f,
            total_chunks=1
        )
        
    print("\\nTodas as ingestões concluídas! Rodando Profiler final...")
    from signature_profiler import SignatureProfiler
    profiler = SignatureProfiler(native_db_path, "./data/profile_8months.json")
    profile = profiler.build_profile(
        symbol="6J",
        lookback_days=60,
        horizon_minutes=5,
        since="2025-10-01" 
    )
    profiler.save(profile)
    print("Profiler concluído com sucesso!")

if __name__ == "__main__":
    main()
