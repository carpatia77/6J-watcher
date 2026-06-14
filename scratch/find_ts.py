import duckdb
conn = duckdb.connect('/home/aidea/data_backtest/backtest_2025_train.db', read_only=True)
q = '''
SELECT timestamp_ns, timestamp, json_extract_string(raw_payload, '$.cancel_bid_vol') as cancel
FROM liquidity_clusters 
WHERE behavior_signature = 'spoofing_bid_pull' 
AND timestamp >= '2025-10-23 20:00:00'::TIMESTAMP 
AND timestamp <= '2025-10-23 22:00:00'::TIMESTAMP
AND CAST(json_extract_string(raw_payload, '$.cancel_bid_vol') AS INT) = 336
'''
print(conn.execute(q).df().to_string(index=False))
