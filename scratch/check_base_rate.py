import duckdb
conn = duckdb.connect('/home/aidea/data_backtest/backtest_2025_train.db', read_only=True)
query = '''
SELECT
    COUNT(*) AS total_spoofing_outubro,
    ROUND(COUNT(*) / 26.0, 1) AS eventos_por_dia,
    ROUND(COUNT(*) / (26.0 * 24.0), 1) AS eventos_por_hora
FROM liquidity_clusters
WHERE symbol = '6J'
  AND timestamp BETWEEN '2025-10-05' AND '2025-10-31'
  AND behavior_signature IN ('spoofing_bid_pull', 'spoofing_ask_pull')
'''
print(conn.execute(query).df().to_string(index=False))
