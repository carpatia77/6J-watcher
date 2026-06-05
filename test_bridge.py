import requests
import json

def test_mql_bridge_synthetic_payload():
    payload = {
        "symbol": "6J",
        "timestamp": "2026-06-05 14:30:00",
        "tape": [
            {"timestamp": "2026-06-05 14:30:00", "price": 150.250, "volume": 100, "side": "buy"}
        ],
        "dom": [
            {"timestamp": "2026-06-05 14:30:00", "price": 150.250, "level_index": 0, "bid_volume": 50, "ask_volume": 30}
        ]
    }
    
    print("Enviando payload sintético simulando a MQL5 Bridge...")
    try:
        response = requests.post("http://127.0.0.1:8765/ingest", json=payload, timeout=5)
        print(f"Status Code: {response.status_code}")
        print("Resposta do Sentinel:")
        print(json.dumps(response.json(), indent=2, ensure_ascii=False))
    except Exception as e:
        print(f"Erro na conexão com o Sentinel: {e}")
        print("Certifique-se de que o main.py (FastAPI) está rodando na porta 8765.")

if __name__ == "__main__":
    test_mql_bridge_synthetic_payload()
