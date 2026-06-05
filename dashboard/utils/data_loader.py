import requests
from typing import Dict, List, Optional

BASE_URL = "http://127.0.0.1:8765"

def _get(endpoint: str, params: Optional[Dict] = None, timeout: int = 5) -> Dict:
    """Wrapper GET com tratamento de erro padrão."""
    try:
        r = requests.get(f"{BASE_URL}{endpoint}", params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def load_powermeter(symbol: str = "6J", window_seconds: int = 30) -> Dict:
    return _get("/powermeter", {"symbol": symbol, "window_seconds": window_seconds})

def load_tape_live(symbol: str = "6J", limit: int = 15) -> Dict:
    return _get("/tape/live", {"symbol": symbol, "limit": limit})

def load_hotspots(symbol: str = "6J", min_occurrences: int = 3) -> Dict:
    return _get("/hotspots", {"symbol": symbol, "min_occurrences": min_occurrences})

def load_dom_snapshot(symbol: str = "6J", delta_minutes: int = 2) -> Dict:
    return _get("/dom/snapshot", {"symbol": symbol, "delta_minutes": delta_minutes})

def load_confluences(symbol: str = "6J") -> Dict:
    return _get("/confluences", {"symbol": symbol})

def load_report(symbol: str = "6J") -> Dict:
    # /report retorna texto (markdown), não JSON
    try:
        r = requests.get(f"{BASE_URL}/report", params={"symbol": symbol}, timeout=10)
        r.raise_for_status()
        return {"report": r.text}
    except Exception as e:
        return {"error": str(e)}
