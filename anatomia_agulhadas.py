import duckdb
import pandas as pd
import sys
sys.path.insert(0, '.')
from didi_tracker import calcular_didi_index, detectar_agulhadas

conn = duckdb.connect('/home/aidea/data_backtest/backtest_tmp_read.db', 
                      read_only=True)

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
        SELECT
            behavior_signature,
            session,
            COUNT(*) AS eventos,
            SUM(cumdelta) AS cumdelta_sum,
            SUM(total_bid + total_ask) AS vol_total,
            AVG(confidence) AS conf_media,
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

    resultados.append({
        'timestamp': ts,
        'close': ag['close'],
        'dist_norm': round(ag['dist_normalizadora'], 6),
        'qualidade': ag['qualidade'],
        'score_fp': score,
        'regime': regime,
        'n_sigs': len(fp)
    })

df = pd.DataFrame(resultados)

pd.set_option('display.max_rows', 50)
pd.set_option('display.width', 130)
print('=== ANATOMIA DAS AGULHADAS BUY — 8 MESES ===')
print(df.sort_values('dist_norm').to_string())
print()
print('=== RESUMO POR QUALIDADE ===')
print(df.groupby(['qualidade','regime']).agg(
    n=('timestamp','count'),
    score_medio=('score_fp','mean'),
    score_max=('score_fp','max')
).to_string())
conn.close()

# Adicionar resultado forward 5 dias
conn2 = duckdb.connect('/home/aidea/data_backtest/backtest_tmp_read.db', read_only=True)

for i, row in df.iterrows():
    ts = row['timestamp']
    fwd = conn2.execute(f"""
        SELECT 
            MAX(price) - FIRST(price ORDER BY timestamp) AS mfe_5d,
            MIN(price) - FIRST(price ORDER BY timestamp) AS mae_5d
        FROM liquidity_clusters
        WHERE symbol = '6J'
          AND timestamp BETWEEN '{ts}'::TIMESTAMP 
                            AND '{ts}'::TIMESTAMP + INTERVAL '5 days'
    """).df()
    if not fwd.empty:
        df.at[i, 'mfe_5d_ticks'] = round(fwd['mfe_5d'].iloc[0] / 0.00005, 1)
        df.at[i, 'mae_5d_ticks'] = round(fwd['mae_5d'].iloc[0] / 0.00005, 1)

conn2.close()

print()
print('=== RESULTADO FORWARD 5 DIAS (ticks) ===')
cols = ['timestamp','qualidade','score_fp','regime','mfe_5d_ticks','mae_5d_ticks']
print(df[cols].sort_values('score_fp', ascending=False).to_string())
