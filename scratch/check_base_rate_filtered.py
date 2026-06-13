import duckdb
conn = duckdb.connect('/home/aidea/data_backtest/backtest_2025_train.db', read_only=True)
query = '''
SELECT
    'Todos (>= 38)' as threshold,
    COUNT(*) AS total,
    ROUND(COUNT(*) / (26.0 * 24.0), 1) AS por_hora
FROM liquidity_clusters
WHERE symbol = '6J' AND timestamp BETWEEN '2025-10-05' AND '2025-10-31'
  AND behavior_signature IN ('spoofing_bid_pull', 'spoofing_ask_pull')
UNION ALL
SELECT
    '>= 100 lotes' as threshold,
    COUNT(*) AS total,
    ROUND(COUNT(*) / (26.0 * 24.0), 1) AS por_hora
FROM liquidity_clusters
WHERE symbol = '6J' AND timestamp BETWEEN '2025-10-05' AND '2025-10-31'
  AND behavior_signature IN ('spoofing_bid_pull', 'spoofing_ask_pull')
  AND CAST(json_extract_string(raw_payload, '$.cancel_bid_vol') AS INT) >= 100
UNION ALL
SELECT
    '>= 150 lotes' as threshold,
    COUNT(*) AS total,
    ROUND(COUNT(*) / (26.0 * 24.0), 1) AS por_hora
FROM liquidity_clusters
WHERE symbol = '6J' AND timestamp BETWEEN '2025-10-05' AND '2025-10-31'
  AND behavior_signature IN ('spoofing_bid_pull', 'spoofing_ask_pull')
  AND CAST(json_extract_string(raw_payload, '$.cancel_bid_vol') AS INT) >= 150
UNION ALL
SELECT
    '>= 200 lotes' as threshold,
    COUNT(*) AS total,
    ROUND(COUNT(*) / (26.0 * 24.0), 1) AS por_hora
FROM liquidity_clusters
WHERE symbol = '6J' AND timestamp BETWEEN '2025-10-05' AND '2025-10-31'
  AND behavior_signature IN ('spoofing_bid_pull', 'spoofing_ask_pull')
  AND CAST(json_extract_string(raw_payload, '$.cancel_bid_vol') AS INT) >= 200
'''
print(conn.execute(query).df().to_string(index=False))
