import requests
from datetime import datetime

payload = {
    "symbol": "6J",
    "tape": [{"timestamp": datetime.utcnow().isoformat(), "price": 0.67250, "volume": 50, "side": "buy"}],
    "dom": [{"timestamp": datetime.utcnow().isoformat(), "price": 0.67250, "level_index": 0, "bid_volume": 100, "ask_volume": 80}],
}

try:
    r = requests.post("http://127.0.0.1:8765/ingest", json=payload)
    print("Ingest status:", r.status_code)
    print("Ingest response:", r.json())
    
    pm = requests.get("http://127.0.0.1:8765/powermeter?symbol=6J&window_seconds=30")
    print("Powermeter status:", pm.status_code)
    print("Powermeter response:", pm.json())
except Exception as e:
    print("Error:", e)
