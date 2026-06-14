import duckdb
conn = duckdb.connect('/home/aidea/data_backtest/backtest_2025_train.db', read_only=True)
print("=== tape_events schema ===")
print(conn.execute('DESCRIBE tape_events').df().to_string())
print("\n=== timestamp range ===")
print(conn.execute('SELECT MIN(timestamp), MAX(timestamp) FROM tape_events').df().to_string())
conn.close()
