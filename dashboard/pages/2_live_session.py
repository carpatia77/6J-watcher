import streamlit as st
from streamlit_autorefresh import st_autorefresh
import sys
from pathlib import Path

# Add parent dir to path so we can import utils
sys.path.append(str(Path(__file__).parent.parent))

from utils.data_loader import (
    load_powermeter, load_tape_live,
    load_hotspots, load_dom_snapshot,
)
from components.powermeter import render_powermeter
from components.tape_live import render_tape_live
from components.dom_snapshot import render_dom_snapshot

st.set_page_config(page_title="Live Session | 6J Watcher", page_icon="🔴", layout="wide")
st.title("🔴 6J Watcher — Live Session Monitor")

# ── Controles ───────────────────────────────────────────────
col_window, col_refresh, col_symbol = st.columns([1, 2, 1])
with col_window:
    window_seconds = st.selectbox(
        "Janela Powermeter",
        options=[5, 15, 30, 60, 120],
        index=2,
        format_func=lambda x: f"{x}s",
    )
with col_refresh:
    auto_refresh = st.checkbox("Auto-refresh (10s)", value=True)
with col_symbol:
    symbol = st.text_input("Símbolo", value="6J")

# Auto-refresh moderno (substitui time.sleep + st.rerun)
if auto_refresh:
    st_autorefresh(interval=10000, key="live_refresh")

st.markdown("---")

# ── Layout 2x2 ──────────────────────────────────────────────
col_left_top, col_right_top = st.columns(2)

with col_left_top:
    st.markdown("### 📼 Tape Live + ⚡ Powermeter")
    pm_data = load_powermeter(symbol, window_seconds)
    render_powermeter(pm_data)
    st.markdown("---")
    tape_data = load_tape_live(symbol, limit=15)
    if "error" in tape_data:
        st.error(tape_data["error"])
    else:
        render_tape_live(tape_data.get("events", []))

with col_right_top:
    st.markdown("### 📚 DOM Snapshot")
    dom_data = load_dom_snapshot(symbol, delta_minutes=2)
    render_dom_snapshot(dom_data)

st.markdown("---")

col_left_bottom, col_right_bottom = st.columns(2)

with col_left_bottom:
    st.markdown("### 🎯 Hotspots Ativos (sessão)")
    hs = load_hotspots(symbol, min_occurrences=3)
    if "error" in hs:
        st.error(hs["error"])
    else:
        hotspots = hs.get("hotspots", [])[:10]
        if hotspots:
            st.dataframe(
                [
                    {
                        "Preço": f"{h['price']:.5f}",
                        "Tier": h.get("tier", "?"),
                        "Assinatura": h.get("dominant_signature", "?"),
                        "Ocorr.": h["occurrences"],
                        "Win%": f"{h.get('win_rate', 0):.0%}",
                    }
                    for h in hotspots
                ],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Sem hotspots ativos ainda.")

with col_right_bottom:
    st.markdown("### 🔔 Alertas Pendentes")
    st.info("🚧 Sprint 4: fila de smart alerts do Narrator")
