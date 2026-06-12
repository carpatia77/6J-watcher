import duckdb
from queries import build_mfe_mae_cte
from signature_profiler import SignatureProfiler

db_path = "data/backtest_8months.db"
conn = duckdb.connect(db_path, read_only=True)

base_cte = build_mfe_mae_cte(
    symbol='6J',
    start_date='2026-05-01',
    horizon_minutes=30,
    is_sampling=True,
    sample_size=20000,
)

depth_sql = base_cte + """
,
win_by_band AS (
    SELECT
        behavior_signature AS sig,
        CASE 
            WHEN dom_min_level <= 2 THEN 'shallow'
            WHEN dom_min_level <= 5 THEN 'mid'
            ELSE 'deep'
        END AS depth_band,
        COUNT(*) AS cnt,
        SUM(win) * 1.0 / COUNT(*) AS win_rate
    FROM scored
    GROUP BY behavior_signature, depth_band
)
SELECT sig, depth_band, cnt, win_rate
FROM win_by_band
ORDER BY sig, depth_band
"""

try:
    df = conn.execute(depth_sql).fetchdf()
    print(df)
except Exception as e:
    print(e)
