import streamlit as st

st.set_page_config(
    page_title="6J Watcher",
    page_icon="🐋",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🐋 6J Watcher — Liquidity Intelligence Platform")

st.markdown("""
## Bem-vindo

Plataforma de inteligência institucional para Order Flow no CME 6J (USDJPY futures).

Selecione uma das abas no menu lateral:

- **📊 Pré-Session** — Análise preparatória (heatmap, hotspots, confluências, narrativa LLM)
- **🔴 Live Session** — Monitoramento em tempo real (Tape, Powermeter, DOM, Hotspots ativos)

---

**Status do Backend:**
""")

# Verifica se o backend está respondendo
try:
    import requests
    r = requests.get("http://127.0.0.1:8765/health", timeout=2)
    if r.status_code == 200:
        data = r.json()
        st.success(
            f"✅ Backend OK | {data['matrix_levels']} níveis ativos | "
            f"DB: {data['db_size_mb']} MB | Uptime: {data['uptime_seconds']}s"
        )
    else:
        st.warning(f"⚠️ Backend respondeu status {r.status_code}")
except Exception as e:
    st.error(f"❌ Backend offline: {e}. Execute `python main.py`.")
