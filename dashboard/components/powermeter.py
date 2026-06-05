import streamlit as st
from typing import Dict

def render_powermeter(data: Dict):
    """Renderiza Powermeter visual (substitui delta textual)."""
    if "error" in data:
        st.error(f"Erro ao carregar Powermeter: {data['error']}")
        return

    total = data["buy_volume"] + data["sell_volume"]
    if total == 0:
        st.info(f"⚡ Aguardando fluxo de ordens (janela: {data['window_seconds']}s)...")
        return

    buy = data["buy_volume"]
    sell = data["sell_volume"]
    buy_pct = (buy / total) * 100

    st.markdown(
        f"#### ⚡ POWERMETER (últimos {data['window_seconds']}s)"
    )

    # Labels SELL / BUY
    col_sell, col_bar, col_buy = st.columns([3, 6, 3])
    with col_sell:
        st.markdown(
            f"<div style='text-align:right; color:#F23645; font-weight:bold; "
            f"font-family:monospace; font-size:1.1em'>SELL {sell}</div>",
            unsafe_allow_html=True,
        )
    with col_bar:
        st.markdown(
            f"""
            <div style='background: linear-gradient(to right,
                #F23645 0%, #F23645 {100-buy_pct:.1f}%,
                #089981 {100-buy_pct:.1f}%, #089981 100%);
                height: 28px; border-radius: 4px;
                border: 1px solid #444; position: relative;'>
                <div style='position: absolute; left: 50%; top: 50%;
                    transform: translate(-50%, -50%);
                    color: white; font-weight: bold;
                    text-shadow: 1px 1px 2px black;'>
                    {data['delta']:+d}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col_buy:
        st.markdown(
            f"<div style='text-align:left; color:#089981; font-weight:bold; "
            f"font-family:monospace; font-size:1.1em'>BUY {buy}</div>",
            unsafe_allow_html=True,
        )

    # Métricas auxiliares
    col_dom, col_trend, col_trades = st.columns(3)
    with col_dom:
        dom_color = (
            "#089981" if data["dominant"] == "COMPRA"
            else "#F23645" if data["dominant"] == "VENDA"
            else "#888"
        )
        st.markdown(
            f"Dominante: <span style='color:{dom_color}; font-weight:bold'>"
            f"{data['dominant']}</span> ({data['dominant_pct']:.0f}%)",
            unsafe_allow_html=True,
        )
    with col_trend:
        trend_color = (
            "#089981" if data["trend"] > 0
            else "#F23645" if data["trend"] < 0
            else "#888"
        )
        st.markdown(
            f"Tendência: <span style='color:{trend_color}; font-weight:bold'>"
            f"{data['trend_icon']} {data['trend_label']}</span> ({data['trend']:+d})",
            unsafe_allow_html=True,
        )
    with col_trades:
        st.markdown(f"Trades: **{data['trade_count']}**")
