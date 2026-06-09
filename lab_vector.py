import time
import pandas as pd
import numpy as np

def sandbox_vector_test():
    print("=== Sandbox: Teste de Vetorizacao do Pipeline ===")
    
    # 1. Gerar dados sintéticos simulando o lote que chega do Databento (dicts)
    raw_tape = [
        {"timestamp_ns": 100_000_000, "price": 10.0, "volume": 5, "side": "buy"},
        {"timestamp_ns": 200_000_000, "price": 10.5, "volume": 10, "side": "buy"},
        # Pula para proxima janela (janela corta a cada 250_000_000 ns)
        {"timestamp_ns": 300_000_000, "price": 11.0, "volume": 3, "side": "sell"},
        {"timestamp_ns": 400_000_000, "price": 10.0, "volume": 7, "side": "sell"},
        {"timestamp_ns": 450_000_000, "price": 9.5,  "volume": 2, "side": "buy"},
    ]
    
    print(f"Total eventos de fita (simulado): {len(raw_tape)}")
    t0 = time.perf_counter()
    
    # 2. Conversão para Pandas DataFrame (extremamente rápido no C back-end)
    df = pd.DataFrame(raw_tape)
    
    # 3. Criar window_id identificando a janela de 250ms
    df['window_id'] = df['timestamp_ns'] // 250_000_000
    
    # 4. Vetorização do sinal de volume (buy=+ / sell=-)
    df['signed_vol'] = np.where(df['side'].str.lower().isin(['buy', 'b']), df['volume'], -df['volume'])
    df['bid_vol']    = np.where(df['signed_vol'] > 0, df['volume'], 0)
    df['ask_vol']    = np.where(df['signed_vol'] < 0, df['volume'], 0)
    
    # Cumdelta intra-janela (CVD incremental)
    df['cumdelta'] = df.groupby('window_id')['signed_vol'].cumsum()
    
    # 5. Redução/Agrupamento: de milhares de eventos para poucos Clusters
    agg_funcs = {
        'timestamp_ns': 'first',
        'price': ['first', 'last'],
        'bid_vol': 'sum',
        'ask_vol': 'sum',
        'cumdelta': ['min', 'max', 'last']
    }
    
    clusters = df.groupby('window_id').agg(agg_funcs)
    
    # Achatando os nomes das colunas
    clusters.columns = ['_'.join(col).strip() for col in clusters.columns.values]
    
    # Delta Price (last - first)
    clusters['delta_price'] = clusters['price_last'] - clusters['price_first']
    
    t_elapsed = time.perf_counter() - t0
    
    print("\n--- DataFrame de Clusters Agregados ---")
    print(clusters.to_string())
    print(f"\nTempo processamento: {t_elapsed:.6f}s")

if __name__ == "__main__":
    sandbox_vector_test()
