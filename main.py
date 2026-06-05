from __future__ import annotations
import json
import time
import os
import threading
from datetime import datetime, timedelta

import uvicorn
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional

from config import Config
from repository_duckdb import DuckDBRepository
from liquidity_matrix import LiquidityMatrix
from adaptive_pattern_engine import AdaptivePatternEngine  # não mais PatternEngine
from narrator import Narrator
from ingestion import IngestionService

# ── Inicialização ─────────────────────────────────────────────
cfg = Config()
repo = DuckDBRepository(cfg.db_path)
matrix = LiquidityMatrix(cfg.symbol, cfg.tick_size)
engine = AdaptivePatternEngine(cfg=cfg)
narrator = Narrator(engine=engine, cfg=cfg)
service = IngestionService(repo, matrix, engine, cfg, narrator=narrator) # Adicionado narrator
start_time = time.time()

# ── FastAPI App ───────────────────────────────────────────────
app = FastAPI(title="6J Watcher API", version="2.0.0")

# CORS obrigatório para Streamlit (porta 8501) fazer requests ao backend (8765)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Em produção, restringir para a URL do Streamlit
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Pydantic Models ───────────────────────────────────────────
class IngestPayload(BaseModel):
    symbol: str
    tape: List[Dict]
    dom: List[Dict]
    timestamp: Optional[str] = None

# ── Endpoints ─────────────────────────────────────────────────
@app.post("/ingest")
def ingest(payload: IngestPayload):
    """Recebe payload do MQL5 Bridge e processa via IngestionService."""
    try:
        clusters = service.ingest_batch(payload.tape, payload.dom, payload.symbol)
        narrator.invalidate_cache()
        return {
            "status": "ok",
            "clusters": len(clusters),
            "hotspots": matrix.hotspots(cfg.min_occurrences),
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}, 500

@app.get("/hotspots")
def get_hotspots(
    symbol: str = Query(None),
    min_occurrences: int = Query(3, ge=1)
):
    sym = symbol or cfg.symbol
    hotspots = matrix.hotspots(min_occurrences)
    # Enriquece com qualidade do sinal
    for h in hotspots:
        sig = h.get("dominant_signature", "unknown")
        sess = h.get("session", "NEW_YORK")
        quality = engine.get_signal_quality(sig, sess)
        h["win_rate"] = quality["historical_win_rate"]
        h["profit_factor"] = quality["profit_factor"]
        h["tier"] = quality["tier"]
        h["sample_size"] = quality["sample_size"]
    return {"symbol": sym, "hotspots": hotspots}

@app.get("/report")
def get_report(symbol: str = Query(None)):
    sym = symbol or cfg.symbol
    hotspots = matrix.hotspots(cfg.min_occurrences)
    sig_dist = repo.signature_distribution(sym)
    sess_analysis = repo.session_analysis(sym)
    return narrator.daily_report(sym, hotspots, sig_dist, sess_analysis)

@app.get("/confluences")
def get_confluences(symbol: str = Query(None)):
    sym = symbol or cfg.symbol
    hotspots = matrix.hotspots(cfg.min_occurrences)
    return {"confluences": narrator.detect_confluences(hotspots)}

@app.get("/powermeter")
def get_powermeter(
    symbol: str = Query(None),
    window_seconds: int = Query(30, ge=5, le=300)
):
    """Pressão compradora vs vendedora com tendência vs janela anterior."""
    sym = symbol or cfg.symbol
    now = datetime.utcnow()
    cutoff = now - timedelta(seconds=window_seconds)
    prev_cutoff = cutoff - timedelta(seconds=window_seconds)

    buy_volume = sell_volume = 0
    prev_buy = prev_sell = 0
    trade_count = 0

    # IMPORTANTE: atributo correto é tape_events (não tape_index)
    with matrix.lock:
        for price, buckets in matrix.tape_events.items() if hasattr(matrix, 'tape_events') else matrix.tape_index.items():
            for bucket, tapes in buckets.items():
                for tape in tapes:
                    ts = tape.timestamp
                    if ts >= cutoff:
                        trade_count += 1
                        if tape.side.value == "buy":
                            buy_volume += tape.volume
                        else:
                            sell_volume += tape.volume
                    elif ts >= prev_cutoff:
                        if tape.side.value == "buy":
                            prev_buy += tape.volume
                        else:
                            prev_sell += tape.volume

    current_delta = buy_volume - sell_volume
    prev_delta = prev_buy - prev_sell
    trend = current_delta - prev_delta
    total_volume = buy_volume + sell_volume

    if total_volume == 0:
        dominant, dominant_pct = "NEUTRO", 50.0
    else:
        buy_pct = (buy_volume / total_volume) * 100
        dominant_pct = max(buy_pct, 100 - buy_pct)
        dominant = (
            "COMPRA" if dominant_pct >= 55 and buy_pct > 50
            else "VENDA" if dominant_pct >= 55
            else "EQUILÍBRIO"
        )

    if abs(trend) < total_volume * 0.1:
        trend_label, trend_icon = "ESTÁVEL", "→"
    elif trend > 0:
        trend_label, trend_icon = "AUMENTANDO", "↗"
    else:
        trend_label, trend_icon = "DIMINUINDO", "↘"

    return {
        "symbol": sym,
        "buy_volume": buy_volume,
        "sell_volume": sell_volume,
        "delta": current_delta,
        "trend": trend,
        "trend_label": trend_label,
        "trend_icon": trend_icon,
        "dominant": dominant,
        "dominant_pct": dominant_pct,
        "trade_count": trade_count,
        "window_seconds": window_seconds,
        "timestamp": now.isoformat(),
    }

@app.get("/tape/live")
def get_tape_live(
    symbol: str = Query(None),
    limit: int = Query(15, ge=1, le=100)
):
    """Eventos recentes da fita."""
    sym = symbol or cfg.symbol
    events = []
    with matrix.lock:
        tapes = getattr(matrix, 'tape_events', getattr(matrix, 'tape_index', {}))
        for price, buckets in tapes.items():
            for bucket, t_list in buckets.items():
                for tape in t_list:
                    # Resolve cluster correspondente
                    signature = "—"
                    if hasattr(matrix, 'matrix'):
                        clusters = matrix.matrix.get(price, {}).get(bucket, [])
                        for c in clusters:
                            if c.timestamp == tape.timestamp:
                                signature = c.behavior_signature.value
                                break
                    
                    events.append({
                        "price": tape.price,
                        "side": tape.side.value,
                        "volume": tape.volume,
                        "delta_ticks": 0,
                        "signature": signature,
                        "timestamp": tape.timestamp
                    })
                    
    events.sort(key=lambda x: x["timestamp"], reverse=True)
    return {"events": events[:limit]}

@app.get("/dom/snapshot")
def get_dom_snapshot(
    symbol: str = Query(None),
    delta_minutes: int = Query(2, ge=1, le=10),
    levels: int = Query(10, ge=5, le=20)
):
    """DOM atual + delta vs N minutos atrás. Detecta icebergs."""
    sym = symbol or cfg.symbol
    now = datetime.utcnow()
    cutoff_current = now - timedelta(minutes=1)
    cutoff_past = now - timedelta(minutes=delta_minutes + 1)

    current_dom = {}  # price -> {bid, ask}
    past_dom = {}

    with matrix.lock:
        for price, buckets in matrix.dom_snapshots.items():
            for bucket, doms in buckets.items():
                for d in doms:
                    if d.timestamp >= cutoff_current:
                        current_dom.setdefault(d.price, {"bid": 0, "ask": 0})
                        current_dom[d.price]["bid"] += d.bid_volume
                        current_dom[d.price]["ask"] += d.ask_volume
                    elif d.timestamp >= cutoff_past:
                        past_dom.setdefault(d.price, {"bid": 0, "ask": 0})
                        past_dom[d.price]["bid"] += d.bid_volume
                        past_dom[d.price]["ask"] += d.ask_volume

    # Calcula deltas e ordena
    snapshot = []
    for price in set(current_dom.keys()) | set(past_dom.keys()):
        cur = current_dom.get(price, {"bid": 0, "ask": 0})
        pas = past_dom.get(price, {"bid": 0, "ask": 0})
        snapshot.append({
            "price": price,
            "bid_volume": cur["bid"],
            "ask_volume": cur["ask"],
            "bid_delta": cur["bid"] - pas["bid"],
            "ask_delta": cur["ask"] - pas["ask"],
        })

    snapshot.sort(key=lambda x: x["price"], reverse=True)

    return {
        "symbol": sym,
        "delta_minutes": delta_minutes,
        "levels": snapshot[:levels * 2],
    }

@app.get("/health")
def health():
    """Endpoint de health check para monitoramento."""
    try:
        db_size_mb = os.path.getsize(cfg.db_path) / 1e6 if os.path.exists(cfg.db_path) else 0
    except:
        db_size_mb = 0
    return {
        "status": "ok",
        "matrix_levels": len(matrix.active_levels),
        "db_size_mb": round(db_size_mb, 2),
        "uptime_seconds": int(time.time() - start_time),
    }

# ── Background Scheduler ─────────────────────────────────────
def background_scheduler():
    """Executa manutenção periódica da matriz (Tier 2 da auditoria)."""
    while True:
        time.sleep(1800)  # 30 minutos
        try:
            matrix.prune_stale_data(hours=4)
        except Exception as e:
            print(f"[scheduler] prune error: {e}")

# ── Main ──────────────────────────────────────────────────────
def main():
    threading.Thread(target=background_scheduler, daemon=True).start()
    print(f"[6J Watcher] FastAPI server running on {cfg.host}:{cfg.port}")
    print(f"[6J Watcher] Docs em http://{cfg.host}:{cfg.port}/docs")
    uvicorn.run(app, host=cfg.host, port=cfg.port, log_level="info")

if __name__ == "__main__":
    main()
