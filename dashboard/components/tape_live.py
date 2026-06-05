import streamlit as st
from typing import List, Dict

def render_tape_live(events: List[Dict]):
    """Renderiza últimos eventos da fita (tape)."""
    if not events:
        st.caption("Sem eventos recentes.")
        return

    # Formatação para tabela
    rows = []
    for e in events:
        sig = e.get("signature") or "—"
        sig_icon = {
            "absorption_passive": "⚡ ABSORPTION",
            "defense_line": "🔴 DEFENSE",
            "breakout_genuine": "🚀 BREAKOUT",
            "iceberg_accumulation": "🧊 ICEBERG↑",
            "iceberg_distribution": "🧊 ICEBERG↓",
        }.get(sig, sig)
        rows.append({
            "Preço": f"{e['price']:.5f}",
            "Side": e["side"].upper(),
            "Vol": e["volume"],
            "Δt": e.get("delta_ticks", 0),
            "Assinatura": sig_icon,
        })

    st.dataframe(rows, use_container_width=True, hide_index=True)
