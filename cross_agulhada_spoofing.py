import duckdb
import pandas as pd
import numpy as np
import sys
from datetime import timedelta
import os

# Adiciona o diretorio raiz para importar didi_tracker
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from didi_tracker import calcular_didi_index, detectar_agulhadas

def main():
    db_path = '/home/aidea/data_backtest/backtest_2025_train.db'
    print(f"Conectando ao banco {db_path}...")
    conn = duckdb.connect(db_path, read_only=True)

    # 1. Gerar OHLCV a partir dos clusters (proxy para tape)
    print("Gerando velas H1 (OHLCV) a partir de liquidity_clusters...")
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
        SELECT
            timestamp,
            ROUND(1.0 / close_6j,  3) AS open,
            ROUND(1.0 / low_6j,    3) AS high,
            ROUND(1.0 / high_6j,   3) AS low,
            ROUND(1.0 / close_6j,  3) AS close,
            volume
        FROM raw
        ORDER BY timestamp
    """).df()

    print(f"Geradas {len(ohlcv)} velas H1.")

    # 2. Calcular Agulhadas
    print("Calculando Index Didi e detectando Agulhadas...")
    didi = calcular_didi_index(ohlcv)
    # Procuramos agulhadas de compra (buy) sem limite de normalizacao para varredura completa
    agulhadas = detectar_agulhadas(didi, direcao='buy', max_dist_normalizadora=9999)
    
    # Filtra apenas a qualidade ELITE e ALTA para teste (score=6 equivalente)
    agulhadas_elite = agulhadas[agulhadas['qualidade'].isin(['ELITE', 'ALTA', 'MEDIA'])]
    print(f"Agulhadas detectadas (ELITE/ALTA/MEDIA): {len(agulhadas_elite)}")

    # 3. Cruzar com Spoofing Pulls no banco de dados
    print("\nCruzando Agulhadas com eventos de 'spoofing_bid_pull' num raio de ±60 minutos...")
    
    resultados = []
    
    for _, ag in agulhadas_elite.iterrows():
        ts_agulhada = ag['timestamp']
        qualidade = ag['qualidade']
        dist = ag['dist_normalizadora']
        
        # Procura spoofings no intervalo [ts_agulhada - 1h, ts_agulhada + 1h]
        # Spoofing Bid Pull => Retirada de muralha de compra no 6J => Distribuição no 6J => Accumulation no USDJPY
        query = f"""
            SELECT 
                timestamp,
                price,
                confidence,
                json_extract_string(raw_payload, '$.cancel_bid_vol')::INT as cancel_bid_vol
            FROM liquidity_clusters
            WHERE behavior_signature = 'spoofing_bid_pull'
              AND timestamp >= '{ts_agulhada}'::TIMESTAMP - INTERVAL '1 hour'
              AND timestamp <= '{ts_agulhada}'::TIMESTAMP + INTERVAL '1 hour'
            ORDER BY cancel_bid_vol DESC
        """
        spoofs = conn.execute(query).df()
        
        n_spoofs = len(spoofs)
        max_cancel = spoofs['cancel_bid_vol'].max() if n_spoofs > 0 else 0
        
        resultados.append({
            'ts_agulhada': ts_agulhada,
            'qualidade': qualidade,
            'dist_norm': round(dist, 6),
            'spoofings_na_janela': n_spoofs,
            'max_cancel_vol': max_cancel,
        })

    df_res = pd.DataFrame(resultados)
    
    if df_res.empty:
        print("\nSem resultados para cruzar.")
    else:
        print("\n=== MAPA DE CONVERGÊNCIA: AGULHADA vs SPOOFING ===")
        df_res = df_res.sort_values('spoofings_na_janela', ascending=False)
        print(df_res.to_string(index=False))
        
        conv = len(df_res[df_res['spoofings_na_janela'] > 0])
        total = len(df_res)
        print(f"\nTaxa de convergência: {conv}/{total} agulhadas ({conv/total*100:.1f}%) tiveram Spoofing associado!")

    conn.close()

if __name__ == '__main__':
    main()
