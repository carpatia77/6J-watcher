import streamlit as st
import sys
from pathlib import Path
# Add parent dir to path so we can import utils
sys.path.append(str(Path(__file__).parent.parent))
from utils.data_loader import load_hotspots, load_confluences, load_report

st.set_page_config(page_title="Pré-Session | 6J Watcher", page_icon="📊", layout="wide")
st.title("📊 6J Watcher — Análise Pré-Session")

st.info("🚧 Sprint 2: Heatmap, Key Levels, Hotspots com Win Rate, Relatório LLM")

st.markdown("---")
st.subheader("⭐ Hotspots (preview)")
data = load_hotspots("6J")
if "error" in data:
    st.error(data["error"])
else:
    st.json(data.get("hotspots", [])[:10])

st.subheader("⚡ Confluências (preview)")
conf = load_confluences("6J")
st.json(conf.get("confluences", []))

st.subheader("📝 Relatório Diário (preview)")
rep = load_report("6J")
if "error" in rep:
    st.error(rep["error"])
else:
    st.markdown(rep.get("report", ""))
