import pandas as pd
import numpy as np

def calcular_didi_index(ohlcv: pd.DataFrame,
                         curta=3, media=8, longa=20,
                         src='close') -> pd.DataFrame:
    df = ohlcv.copy()
    
    sma_media = df[src].rolling(media).mean()
    sma_curta = df[src].rolling(curta).mean()
    sma_longa = df[src].rolling(longa).mean()
    
    df['didi_curta'] = sma_curta / sma_media
    df['didi_media'] = 1.0
    df['didi_longa'] = sma_longa / sma_media
    
    df['dist_curta'] = abs(df['didi_curta'] - 1.0)
    df['dist_longa'] = abs(df['didi_longa'] - 1.0)
    df['dist_normalizadora'] = df['dist_curta'] + df['dist_longa']
    
    df['crossover_buy'] = (
        (df['didi_curta'] > df['didi_longa']) &
        (df['didi_curta'].shift(1) <= df['didi_longa'].shift(1))
    )
    df['crossover_sell'] = (
        (df['didi_curta'] < df['didi_longa']) &
        (df['didi_curta'].shift(1) >= df['didi_longa'].shift(1))
    )
    
    return df


# Thresholds calibrados empiricamente no banco 6J Q4/2025-Jan/2026
THRESHOLDS = {
    'ELITE': 0.000373,   # P25 — compressão máxima (~10 agulhadas/8 meses)
    'ALTA':  0.000705,   # P50 — acima da média  (~20 agulhadas/8 meses)
    'MEDIA': 0.001058,   # P75 — aceitável       (~30 agulhadas/8 meses)
}

def detectar_agulhadas(didi_df: pd.DataFrame,
                        direcao='buy',
                        max_dist_normalizadora=None) -> pd.DataFrame:
    
    if max_dist_normalizadora is None:
        max_dist_normalizadora = THRESHOLDS['ALTA']  # default: P50
    
    col = 'crossover_buy' if direcao == 'buy' else 'crossover_sell'
    agulhadas = didi_df[didi_df[col]].copy()
    
    agulhadas['qualidade'] = pd.cut(
        agulhadas['dist_normalizadora'],
        bins=[0, THRESHOLDS['ELITE'], THRESHOLDS['ALTA'], 
              THRESHOLDS['MEDIA'], np.inf],
        labels=['ELITE', 'ALTA', 'MEDIA', 'BAIXA']
    )
    
    filtradas = agulhadas[
        agulhadas['dist_normalizadora'] <= max_dist_normalizadora
    ]
    
    return filtradas[['timestamp', 'close', 'didi_curta', 'didi_longa',
                       'dist_normalizadora', 'qualidade']].reset_index(drop=True)


def score_qualidade(dist: float) -> str:
    if dist <= THRESHOLDS['ELITE']:
        return 'ELITE'
    elif dist <= THRESHOLDS['ALTA']:
        return 'ALTA'
    elif dist <= THRESHOLDS['MEDIA']:
        return 'MEDIA'
    return 'BAIXA'
