import duckdb
import os

print("Starting DB surgery...")

oos_path = '/home/aidea/data_backtest/backtest_2026_oos.db'
train_path = '/home/aidea/data_backtest/backtest_2025_train.db'

conn_oos = duckdb.connect(oos_path)
conn_oos.execute(f"ATTACH '{train_path}' AS train_db")

conn_oos.execute("""
    CREATE TABLE liquidity_clusters AS 
    SELECT * FROM train_db.liquidity_clusters 
    WHERE timestamp >= '2026-01-01'
""")
print("Data transferred to OOS DB.")

conn_oos.execute("DELETE FROM train_db.liquidity_clusters WHERE timestamp >= '2026-01-01'")
print("Data deleted from Train DB.")

conn_oos.close()
print("Surgery complete!")
