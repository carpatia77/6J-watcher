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

# Merge com OHLCV para pegar high/low/open da vela trigger
agulhadas = agulhadas.merge(
    ohlcv[['timestamp','open','high','low','close','volume']],
    on='timestamp', how='left'
)

resultados = []
for _, ag in agulhadas.iterrows():
    ts = ag['timestamp']
    trigger_open  = ag['open']
    trigger_high  = ag['high']
    trigger_low   = ag['low']
    trigger_body  = trigger_high - trigger_low

    # Nível de entrada: retração de 40% do corpo da vela trigger (BUY)
    entry_target = trigger_high - (trigger_body * 0.40)

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

    # Buscar preços nas 48h após agulhada para encontrar retração
    prices = conn.execute(f"""
        SELECT price, timestamp
        FROM liquidity_clusters
        WHERE symbol = '6J'
          AND timestamp BETWEEN '{ts}'::TIMESTAMP
                            AND '{ts}'::TIMESTAMP + INTERVAL '48 hours'
        ORDER BY timestamp
    """).df()

    entry_price = None
    entry_ts = None
    mfe_ticks = mae_path_ticks = None
    retraction_pct = None

    if not prices.empty:
        # Encontrar primeiro toque no nível de retração 40%
        touch = prices[prices['price'] <= entry_target]
        if not touch.empty:
            entry_price = touch['price'].iloc[0]
            entry_ts = touch['timestamp'].iloc[0]
            retraction_pct = round((trigger_high - entry_price) / trigger_body * 100, 1)

            # MFE/MAE a partir da entrada real — janela 7 dias
            fwd = conn.execute(f"""
                SELECT price FROM liquidity_clusters
                WHERE symbol = '6J'
                  AND timestamp BETWEEN '{entry_ts}'::TIMESTAMP
                                    AND '{entry_ts}'::TIMESTAMP + INTERVAL '7 days'
                ORDER BY timestamp
            """).df()

            if not fwd.empty:
                excursions = fwd['price'] - entry_price
                mfe = excursions.max()
                idx_mfe = excursions.idxmax()
                mae_path = excursions.iloc[:idx_mfe+1].min() if idx_mfe > 0 else 0.0
                mfe_ticks = round(mfe / 0.00005, 1)
                mae_path_ticks = round(mae_path / 0.00005, 1)

    resultados.append({
        'agulhada_ts': ts,
        'entry_ts': entry_ts,
        'entry_price': round(entry_price, 6) if entry_price else None,
        'retraction_pct': retraction_pct,
        'dist_norm': round(ag['dist_normalizadora'], 6),
        'qualidade': str(ag['qualidade']),
        'score_fp': score,
        'regime': regime,
        'mfe_7d': mfe_ticks,
        'mae_path': mae_path_ticks
    })

df = pd.DataFrame(resultados)

pd.set_option('display.max_rows', 50)
pd.set_option('display.width', 150)
print('=== AGULHADAS + ENTRADA NA RETRAÇÃO 40% — FORWARD 7 DIAS ===')
print(df.sort_values('score_fp', ascending=False).to_string())

print()
print('=== RESUMO POR SCORE (RANGING + entrada encontrada) ===')
validas = df[(df['regime'] == 'RANGING') & (df['entry_ts'].notna())]
print(validas.groupby('score_fp').agg(
    n=('agulhada_ts','count'),
    mfe_medio=('mfe_7d','mean'),
    mae_medio=('mae_path','mean'),
    mfe_max=('mfe_7d','max'),
    retraction_media=('retraction_pct','mean')
).round(1).to_string())

conn.close()

# === SWEEP DE THRESHOLD: qual retração mínima maximiza E[MFE] ===
print()
print("=== SWEEP: threshold mínimo de retração vs MFE médio ===")
ranging = df[(df['regime'] == 'RANGING') & (df['entry_ts'].notna())].copy()

for threshold in [38, 45, 50, 55, 60, 65, 70, 75, 80]:
    sub = ranging[ranging['retraction_pct'] >= threshold]
    if len(sub) < 3:
        continue
    print(f"  retração >= {threshold:3d}%  →  n={len(sub):2d}  "
          f"mfe_medio={sub['mfe_7d'].mean():.2f}  "
          f"mae_medio={sub['mae_path'].mean():.2f}  "
          f"mfe_max={sub['mfe_7d'].max():.1f}  "
          f"pct>=1tick={(sub['mfe_7d']>=1.0).mean()*100:.0f}%")
