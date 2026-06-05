import streamlit as st
from typing import Dict

def render_dom_snapshot(data: Dict, last_price: float = None):
    """Renderiza DOM com delta vs N minutos atrás."""
    if "error" in data:
        st.error(f"Erro ao carregar DOM: {data['error']}")
        return

    levels = data.get("levels", [])
    if not levels:
        st.info("Sem dados de DOM recentes.")
        return

    st.markdown(f"#### 📚 DOM Snapshot (Δ vs {data.get('delta_minutes', 2)}min)")

    for lvl in levels:
        price = lvl["price"]
        bid = lvl["bid_volume"]
        ask = lvl["ask_volume"]
        bid_d = lvl["bid_delta"]
        ask_d = lvl["ask_delta"]

        # Destaque se for last price
        is_last = last_price and abs(price - last_price) < 0.00005
        prefix = "🎯 " if is_last else ""

        # Barras visuais (max ~200 para escala)
        scale = 20
        bid_bar = "█" * min(bid // scale, 40)
        ask_bar = "█" * min(ask // scale, 40)

        bid_delta_str = f"{bid_d:+d}" if bid_d else "—"
        ask_delta_str = f"{ask_d:+d}" if ask_d else "—"

        st.markdown(
            f"`{prefix}{price:.5f}` | "
            f"BID **{bid:3d}** ({bid_delta_str}) {bid_bar} | "
            f"ASK **{ask:3d}** ({ask_delta_str}) {ask_bar}"
        )
