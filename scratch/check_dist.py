import duckdb

db_path = '/home/aidea/data_backtest/backtest_2026_oos.db'
conn = duckdb.connect(db_path, read_only=True)

df = conn.execute("SELECT behavior_signature, COUNT(*) as qty FROM liquidity_clusters GROUP BY behavior_signature ORDER BY qty DESC").df()
print(df)

df2 = conn.execute("SELECT behavior_signature, COUNT(*) as qty FROM liquidity_clusters WHERE json_extract_string(raw_payload, '$.cancel_bid_vol')::INT >= 100 GROUP BY behavior_signature ORDER BY qty DESC").df()
print(">= 100L:")
print(df2)

conn.close()
