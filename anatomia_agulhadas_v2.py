import duckdb
import pandas as pd
import sys
sys.path.insert(0, '.')
from didi_tracker import calcular_didi_index, detectar_agulhadas

conn = duckdb.connect('/home/aidea/data_backtest/backtest_tmp_read.db', read_only=True)

ohlcv = conn.execute("""
    SELECT 
        DATE_TRUNC('hour', timestamp) AS timestamp,
        FIRST(price ORDER BY timestamp) AS open,
        MAX(price) AS high,
        MIN(price) AS low,
        LAST(price ORDER BY timestamp) AS close,
        SUM(total_bid + total_ask) AS volume
    FROM liquidity_clusters
    WHERE symbol = '6J' AND session != 'OFF_HOURS'
    GROUP BY 1 ORDER BY 1
""").df()

didi = calcular_didi_index(ohlcv)
agulhadas = detectar_agulhadas(didi, max_dist_normalizadora=9999)

resultados = []
for _, ag in agulhadas.iterrows():
    ts = ag['timestamp']

    # Footprint
    fp = conn.execute(f"""
        WITH with_regime AS (
            SELECT *,
                ABS(
                    AVG(price) OVER (ORDER BY timestamp ROWS BETWEEN 241 PRECEDING AND 1 PRECEDING)
                    - AVG(price) OVER (ORDER BY timestamp ROWS BETWEEN 481 PRECEDING AND 242 PRECEDING)
                ) AS slope_4h
            FROM liquidity_clusters
            WHERE symbol = '6J'
              AND timestamp BETWEEN '{ts}'::TIMESTAMP - INTERVAL '2 hours'
                                AND '{ts}'::TIMESTAMP + INTERVAL '1 hour'
              AND session IN ('LONDON', 'NEW_YORK')
        )
        SELECT behavior_signature, session,
            COUNT(*) AS eventos,
            SUM(cumdelta) AS cumdelta_sum,
            SUM(total_bid + total_ask) AS vol_total,
            CASE WHEN AVG(slope_4h) > 0.00010 THEN 'TRENDING' ELSE 'RANGING' END AS regime
        FROM with_regime
        GROUP BY behavior_signature, session
        ORDER BY vol_total DESC
    """).df()

    score = 0
    regime = 'UNKNOWN'
    if not fp.empty:
        sigs = fp['behavior_signature'].tolist()
        sessions = fp['session'].tolist()
        if 'absorption_passive' in sigs: score += 2
        if 'iceberg_accumulation' in sigs: score += 1
        if 'defense_line' in sigs: score += 1
        if 'spoofing_wall' in sigs: score += 1
        if 'LONDON' in sessions: score += 1
        regime = fp['regime'].iloc[0]

    # Forward 5 dias — buscar série de preços ordenada
    prices = conn.execute(f"""
        SELECT price, timestamp
        FROM liquidity_clusters
        WHERE symbol = '6J'
          AND timestamp BETWEEN '{ts}'::TIMESTAMP
                            AND '{ts}'::TIMESTAMP + INTERVAL '5 days'
        ORDER BY timestamp
    """).df()

    mfe_ticks = mae_ticks = mae_path_ticks = None
    if not prices.empty:
        entry = prices['price'].iloc[0]
        excursions = prices['price'] - entry
        mfe = excursions.max()
        mae = excursions.min()
        
        # MAE no caminho até o MFE (drawdown real)
        idx_mfe = excursions.idxmax()
        mae_path = excursions.iloc[:idx_mfe+1].min() if idx_mfe > 0 else 0.0

        mfe_ticks = round(mfe / 0.00005, 1)
        mae_ticks = round(mae / 0.00005, 1)
        mae_path_ticks = round(mae_path / 0.00005, 1)

    resultados.append({
        'timestamp': ts,
        'close': round(ag['close'], 6),
        'dist_norm': round(ag['dist_normalizadora'], 6),
        'qualidade': str(ag['qualidade']),
        'score_fp': score,
        'regime': regime,
        'mfe_5d': mfe_ticks,
        'mae_5d': mae_ticks,
        'mae_path': mae_path_ticks
    })

df = pd.DataFrame(resultados)

pd.set_option('display.max_rows', 50)
pd.set_option('display.width', 140)
print('=== ANATOMIA DAS AGULHADAS BUY — FORWARD DIRECIONAL ===')
print(df.sort_values('score_fp', ascending=False).to_string())

print()
print('=== MÉDIA POR SCORE (RANGING apenas) ===')
ranging = df[df['regime'] == 'RANGING']
print(ranging.groupby('score_fp').agg(
    n=('timestamp','count'),
    mfe_medio=('mfe_5d','mean'),
    mae_path_medio=('mae_path','mean'),
    mfe_max=('mfe_5d','max')
).round(1).to_string())
conn.close()
