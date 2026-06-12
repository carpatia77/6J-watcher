import duckdb
import pandas as pd
import numpy as np
import sys
sys.path.insert(0, '.')
from didi_tracker import calcular_didi_index, detectar_agulhadas

conn = duckdb.connect('/home/aidea/data_backtest/backtest_8months.db', read_only=True)

# OHLCV em pips USDJPY equivalente (1/price * 10000 → pips)
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
        -- converter para USDJPY (inverso, em pips)
        ROUND(1.0 / close_6j,  3) AS open,
        ROUND(1.0 / low_6j,    3) AS high,   -- low 6J = high USDJPY
        ROUND(1.0 / high_6j,   3) AS low,    -- high 6J = low USDJPY
        ROUND(1.0 / close_6j,  3) AS close,
        volume
    FROM raw
    ORDER BY timestamp
""").df()

didi = calcular_didi_index(ohlcv)
agulhadas = detectar_agulhadas(didi, max_dist_normalizadora=9999)

resultados = []
for _, ag in agulhadas.iterrows():
    ts = ag['timestamp']

    # HIGH/LOW reais da vela trigger em USDJPY pips
    hl = conn.execute(f"""
        SELECT
            ROUND(1.0 / MIN(price), 3) AS high_usd,   -- min 6J = max USDJPY
            ROUND(1.0 / MAX(price), 3) AS low_usd,    -- max 6J = min USDJPY
            ROUND(1.0 / FIRST(price ORDER BY timestamp), 3) AS open_usd
        FROM liquidity_clusters
        WHERE symbol = '6J'
          AND timestamp >= '{ts}'::TIMESTAMP
          AND timestamp <  '{ts}'::TIMESTAMP + INTERVAL '1 hour'
    """).df()

    if hl.empty or hl['high_usd'].iloc[0] is None:
        continue

    trigger_high = hl['high_usd'].iloc[0]   # USDJPY high (pips)
    trigger_low  = hl['low_usd'].iloc[0]    # USDJPY low  (pips)
    trigger_body = trigger_high - trigger_low  # em pips USDJPY

    # Sanity check: corpo >= 5 pips, preço USDJPY plausível
    if trigger_body < 0.05 or trigger_high < 130 or trigger_high > 170:
        continue

    # Footprint (ainda em 6J — lógica de regime não muda)
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
            SUM(total_bid + total_ask) AS vol_total,
            CASE WHEN AVG(slope_4h) > 0.00010 THEN 'TRENDING' ELSE 'RANGING' END AS regime
        FROM with_regime
        GROUP BY behavior_signature, session
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

    # Mínimo USDJPY real nas 12h (= máximo 6J → mínimo USDJPY)
    # Sanity: ±3% do trigger_high USDJPY
    prices_12h = conn.execute(f"""
        SELECT
            ROUND(1.0 / price, 3) AS usdjpy,
            timestamp
        FROM liquidity_clusters
        WHERE symbol = '6J'
          AND timestamp >  '{ts}'::TIMESTAMP
          AND timestamp <= '{ts}'::TIMESTAMP + INTERVAL '12 hours'
          AND (1.0 / price) BETWEEN {trigger_high * 0.97}
                                AND {trigger_high * 1.03}
        ORDER BY timestamp
    """).df()

    if prices_12h.empty:
        continue

    # Retração = mínimo USDJPY nas 12h (preço caiu de trigger_high)
    min_usdjpy    = prices_12h['usdjpy'].min()
    min_usdjpy_ts = prices_12h.loc[prices_12h['usdjpy'].idxmin(), 'timestamp']
    retraction_pct = round((trigger_high - min_usdjpy) / trigger_body * 100, 1)
    body_pips = round(trigger_body, 2)

    # Forward 7 dias a partir do mínimo — em USDJPY
    fwd = conn.execute(f"""
        SELECT ROUND(1.0 / price, 3) AS usdjpy, timestamp
        FROM liquidity_clusters
        WHERE symbol = '6J'
          AND timestamp >= '{min_usdjpy_ts}'::TIMESTAMP
          AND timestamp <= '{min_usdjpy_ts}'::TIMESTAMP + INTERVAL '7 days'
          AND (1.0 / price) BETWEEN {trigger_high * 0.90}
                                AND {trigger_high * 1.10}
        ORDER BY timestamp
    """).df()

    mfe_pips = mae_path_pips = stop_pips = rr = None
    entry_usdjpy = min_usdjpy

    if not fwd.empty:
        excursions   = fwd['usdjpy'] - entry_usdjpy
        mfe          = excursions.max()
        idx_mfe      = excursions.idxmax()
        mae_path     = excursions.iloc[:idx_mfe+1].min() if idx_mfe > 0 else 0.0

        mfe_pips      = round(mfe * 100, 1)        # pips (2 decimais USDJPY)
        mae_path_pips = round(mae_path * 100, 1)

        # Stop = entry abaixo do low USDJPY da vela trigger
        stop_dist  = entry_usdjpy - trigger_low
        stop_pips  = round(stop_dist * 100, 1) if stop_dist > 0 else 0.5
        rr         = round(mfe_pips / stop_pips, 1) if stop_pips > 0 else None

    resultados.append({
        'ts':             ts,
        'qualidade':      str(ag['qualidade']),
        'dist_norm':      round(ag['dist_normalizadora'], 6),
        'score_fp':       score,
        'regime':         regime,
        'body_pips':      body_pips,
        'retraction_pct': retraction_pct,
        'entry_usd':      round(entry_usdjpy, 3),
        'stop_pips':      stop_pips,
        'mfe_7d':         mfe_pips,
        'mae_path':       mae_path_pips,
        'rr':             rr,
    })

df = pd.DataFrame(resultados)
ranging = df[df['regime'] == 'RANGING'].copy()

pd.set_option('display.max_rows', 60)
pd.set_option('display.width', 160)
print('=== RETRAÇÃO REAL EM PIPS USDJPY — ENTRADA NO MÍNIMO 12H ===')
print(ranging.sort_values('retraction_pct').to_string())

print()
print('=== DISTRIBUIÇÃO DE RETRAÇÃO (RANGING) ===')
pcts = ranging['retraction_pct'].dropna()
for p in [10, 25, 50, 75, 90, 95]:
    print(f"  P{p:2d}: {np.percentile(pcts, p):.1f}%")
print(f"  média: {pcts.mean():.1f}%  |  mediana: {pcts.median():.1f}%")

print()
print(f"{'threshold':>10} {'n':>4} {'mfe_med':>8} {'mae_med':>8} "
      f"{'rr_med':>7} {'rr_max':>7} {'%rr>3':>7} {'%rr>5':>7}")
for thr in range(30, 301, 10):
    sub = ranging[ranging['retraction_pct'] >= thr]
    if len(sub) < 3:
        break
    rr_v = sub['rr'].dropna()
    print(f"  >= {thr:3d}%   {len(sub):4d}  "
          f"{sub['mfe_7d'].mean():7.1f}  "
          f"{sub['mae_path'].mean():7.1f}  "
          f"{rr_v.mean():6.1f}  "
          f"{rr_v.max():6.1f}  "
          f"{(rr_v >= 3.0).mean()*100:6.0f}%  "
          f"{(rr_v >= 5.0).mean()*100:6.0f}%")

conn.close()

# === RECALCULAR R:R COM STOP ESTRUTURAL REAL ===
print()
print("=== R:R COM STOP ESTRUTURAL (18 / 20 / 23 pips abaixo do low trigger) ===")

for stop_real in [18, 20, 23]:
    ranging[f'rr_{stop_real}'] = (ranging['mfe_7d'] / stop_real).round(1)

print(ranging[['ts','qualidade','score_fp','body_pips',
               'retraction_pct','mfe_7d','mae_path',
               'rr_18','rr_20','rr_23']].sort_values('score_fp', ascending=False).to_string())

print()
print(f"{'stop':>6} {'n':>4} {'rr_med':>7} {'rr_max':>7} {'%rr>3':>7} {'%rr>5':>7} {'%rr>10':>8}")
for stop_real in [18, 20, 23]:
    col = f'rr_{stop_real}'
    rr_v = ranging[col].dropna()
    print(f"  {stop_real:2d}p   {len(rr_v):4d}  "
          f"{rr_v.mean():6.1f}  "
          f"{rr_v.max():6.1f}  "
          f"{(rr_v >= 3.0).mean()*100:6.0f}%  "
          f"{(rr_v >= 5.0).mean()*100:6.0f}%  "
          f"{(rr_v >= 10.0).mean()*100:7.0f}%")

print()
print("=== SWEEP THRESHOLD vs R:R REAL (stop=20p) ===")
print(f"{'threshold':>10} {'n':>4} {'mfe_med':>8} {'mae_med':>8} {'rr_med':>7} {'rr_max':>7} {'%rr>5':>7} {'%rr>10':>8}")
for thr in range(30, 301, 10):
    sub = ranging[ranging['retraction_pct'] >= thr]
    if len(sub) < 3:
        break
    rr_v = sub['rr_20'].dropna()
    print(f"  >= {thr:3d}%   {len(sub):4d}  "
          f"{sub['mfe_7d'].mean():7.1f}  "
          f"{sub['mae_path'].mean():7.1f}  "
          f"{rr_v.mean():6.1f}  "
          f"{rr_v.max():6.1f}  "
          f"{(rr_v >= 5.0).mean()*100:6.0f}%  "
          f"{(rr_v >= 10.0).mean()*100:7.0f}%")
