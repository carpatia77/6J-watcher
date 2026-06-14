import duckdb
import pandas as pd
import numpy as np
import sys
from datetime import timedelta
import os

# Adiciona o diretorio raiz para importar didi_tracker
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from didi_tracker import calcular_didi_index, detectar_agulhadas

def main():
    db_path = '/home/aidea/data_backtest/backtest_2025_train.db'
    conn = duckdb.connect(db_path, read_only=True)

    ohlcv = conn.execute("""
        WITH raw AS (
            SELECT 
                DATE_TRUNC('hour', timestamp) AS timestamp,
                FIRST(price ORDER BY timestamp)  AS open_6j,
                MAX(price)                        AS high_6j,
                MIN(price)                        AS low_6j,
                LAST(price ORDER BY timestamp)   AS close_6j,
                SUM(total_bid + total_ask)        AS volume,
                session
            FROM liquidity_clusters
            WHERE symbol = '6J' AND session != 'OFF_HOURS'
            GROUP BY 1, 7
        )
        SELECT timestamp, ROUND(1.0 / close_6j, 3) AS open, ROUND(1.0 / low_6j, 3) AS high, ROUND(1.0 / high_6j, 3) AS low, ROUND(1.0 / close_6j, 3) AS close, volume
        FROM raw ORDER BY timestamp
    """).df()

    didi = calcular_didi_index(ohlcv)
    agulhadas = detectar_agulhadas(didi, direcao='buy', max_dist_normalizadora=9999)
    agulhadas_elite = agulhadas[agulhadas['qualidade'].isin(['ELITE', 'ALTA', 'MEDIA'])]

    resultados = []
    
    for _, ag in agulhadas_elite.iterrows():
        ts_agulhada = ag['timestamp']
        
        # Teste Extremo: Janela de ±15 min E cancel_bid >= 200 (2.5 eventos/hora base rate)
        query = f"""
            SELECT 
                timestamp, price, confidence,
                json_extract_string(raw_payload, '$.cancel_bid_vol')::INT as cancel_bid_vol
            FROM liquidity_clusters
            WHERE behavior_signature = 'spoofing_bid_pull'
              AND timestamp >= '{ts_agulhada}'::TIMESTAMP - INTERVAL '15 minutes'
              AND timestamp <= '{ts_agulhada}'::TIMESTAMP + INTERVAL '15 minutes'
              AND json_extract_string(raw_payload, '$.cancel_bid_vol')::INT >= 200
            ORDER BY cancel_bid_vol DESC
        """
        spoofs = conn.execute(query).df()
        
        n_spoofs = len(spoofs)
        max_cancel = spoofs['cancel_bid_vol'].max() if n_spoofs > 0 else 0
        
        resultados.append({
            'ts_agulhada': ts_agulhada,
            'qualidade': ag['qualidade'],
            'dist_norm': round(ag['dist_normalizadora'], 6),
            'spoofings_na_janela_15m_200L': n_spoofs,
            'max_cancel_vol': max_cancel,
        })

    df_res = pd.DataFrame(resultados)
    print("=== CONVERGÊNCIA HARDCORE (±15 min | >= 200 Lotes) ===")
    df_res = df_res.sort_values('spoofings_na_janela_15m_200L', ascending=False)
    print(df_res.to_string(index=False))
    
    conv = len(df_res[df_res['spoofings_na_janela_15m_200L'] > 0])
    total = len(df_res)
    print(f"\nTaxa de convergência Hardcore: {conv}/{total} agulhadas ({conv/total*100:.1f}%)")

    conn.close()

if __name__ == '__main__':
    main()
