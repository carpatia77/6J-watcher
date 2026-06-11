from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
import os

BASE_DIR = Path(__file__).parent

# Carrega .env se existir (para NVIDIA_API_KEY etc.)
_env_path = BASE_DIR / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())

@dataclass
class Config:
    symbol:          str   = "6J"
    tick_size:       float = 0.00005
    data_dir:        str   = field(default_factory=lambda: os.environ.get("DATA_DIR", "/home/aidea/data_backtest"))
    db_path:         str   = field(default_factory=lambda: os.environ.get("DB_PATH", "/home/aidea/data_backtest/backtest_8months.db"))
    cache_dir:       str   = field(default_factory=lambda: os.environ.get("CACHE_DIR", "/home/aidea/data_backtest/databento"))
    host:            str   = "127.0.0.1"
    port:            int   = 8765
    min_occurrences: int   = 3
    window_ns:       int   = 250_000_000
    session_utc: dict = field(default_factory=lambda: {
        "ASIAN":    (0,  8),
        "LONDON":   (8,  13),
        "NEW_YORK": (13, 22),
    })

    # --- Narrator / Alertas ---
    min_alert_win_rate:        float = 0.50
    min_alert_sample_size:     int   = 30
    confluence_tick_tolerance: int   = 20  # ticks de proximidade para confluência

    # --- LLM (NVIDIA API) ---
    nvidia_api_key:      str   = field(default_factory=lambda: os.environ.get("NVIDIA_API_KEY", ""))
    llm_context_model:   str   = "meta/llama-3.1-8b-instruct"
    llm_reasoning_model: str   = "deepseek-ai/deepseek-v4"
    llm_timeout_seconds: float = 5.0
    llm_max_calls_hour:  int   = 100

    # --- Skill paths (Narrator CoT) ---
    profile_path:       str = str(BASE_DIR / "data" / "profile_calibrated.json")
    variance_report_path: str = str(BASE_DIR / "data" / "variance_report.json")

    def session_for(self, hour_utc: int) -> str:
        for name, (start, end) in self.session_utc.items():
            if start <= hour_utc < end:
                return name
        return "OFF_HOURS"
