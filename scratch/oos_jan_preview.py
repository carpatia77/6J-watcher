import duckdb
import pandas as pd
import numpy as np
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from didi_tracker import calcular_didi_index, detectar_agulhadas

def main():
    db_path = '/home/aidea/data_backtest/backtest_2026_oos.db'
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
        
        # Teste 1: Titanium (>= 200L ±15min)
        spoofs_t = conn.execute(f"""
            SELECT json_extract_string(raw_payload, '$.cancel_bid_vol')::INT as cancel_bid_vol
            FROM liquidity_clusters
            WHERE behavior_signature = 'spoofing_bid_pull'
              AND timestamp >= '{ts_agulhada}'::TIMESTAMP - INTERVAL '15 minutes'
              AND timestamp <= '{ts_agulhada}'::TIMESTAMP + INTERVAL '15 minutes'
              AND json_extract_string(raw_payload, '$.cancel_bid_vol')::INT >= 200
        """).df()
        
        # Teste 2: Giant (>= 100L ±15min)
        spoofs_g = conn.execute(f"""
            SELECT json_extract_string(raw_payload, '$.cancel_bid_vol')::INT as cancel_bid_vol
            FROM liquidity_clusters
            WHERE behavior_signature = 'spoofing_bid_pull'
              AND timestamp >= '{ts_agulhada}'::TIMESTAMP - INTERVAL '15 minutes'
              AND timestamp <= '{ts_agulhada}'::TIMESTAMP + INTERVAL '15 minutes'
              AND json_extract_string(raw_payload, '$.cancel_bid_vol')::INT >= 100
        """).df()

        # Teste 3: Standard (>= 100L ±30min)
        spoofs_s = conn.execute(f"""
            SELECT json_extract_string(raw_payload, '$.cancel_bid_vol')::INT as cancel_bid_vol
            FROM liquidity_clusters
            WHERE behavior_signature = 'spoofing_bid_pull'
              AND timestamp >= '{ts_agulhada}'::TIMESTAMP - INTERVAL '30 minutes'
              AND timestamp <= '{ts_agulhada}'::TIMESTAMP + INTERVAL '30 minutes'
              AND json_extract_string(raw_payload, '$.cancel_bid_vol')::INT >= 100
        """).df()
        
        resultados.append({
            'ts_agulhada': ts_agulhada,
            'qualidade': ag['qualidade'],
            'titanium_200L_15m': 1 if len(spoofs_t) > 0 else 0,
            'giant_100L_15m': 1 if len(spoofs_g) > 0 else 0,
            'standard_100L_30m': 1 if len(spoofs_s) > 0 else 0,
        })

    df_res = pd.DataFrame(resultados)
    
    total = len(df_res)
    if total == 0:
        print("Nenhuma agulhada ELITE/ALTA/MEDIA detectada em Janeiro 2026.")
        return

    conv_t = df_res['titanium_200L_15m'].sum()
    conv_g = df_res['giant_100L_15m'].sum()
    conv_s = df_res['standard_100L_30m'].sum()

    print(f"=== SNEAK PEEK OOS: JANEIRO 2026 ===")
    print(f"Total de Agulhadas Encontradas: {total}\n")
    print(f"Sinal Titanium (>= 200L ±15min): {conv_t}/{total} -> {conv_t/total*100:.1f}%")
    print(f"Sinal Giant    (>= 100L ±15min): {conv_g}/{total} -> {conv_g/total*100:.1f}%")
    print(f"Sinal Standard (>= 100L ±30min): {conv_s}/{total} -> {conv_s/total*100:.1f}%\n")
    
    gate_status = "PASS (preliminar)" if (conv_s/total) >= 0.55 else "WARNING (Abaixo do Gate)"
    print(f"STATUS FASE 3 (Standard >= 55%): {gate_status}")

    conn.close()

if __name__ == '__main__':
    main()
