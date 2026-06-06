"""
3_backtest_results.py
---------------------
Aba "Backtest Analytics" — leitura direta do backtest_8months.db.
Atualiza a cada 60s enquanto o orquestrador está rodando em background.
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
from streamlit_autorefresh import st_autorefresh

from utils.backtest_loader import (
    load_summary,
    load_signature_distribution,
    load_session_breakdown,
    load_hotspots_historical,
    load_monthly_progress,
    load_calibrated_profile,
    load_hourly_heatmap,
    load_cumdelta_by_level,
    DEFAULT_DB,
)

# ── Config ──────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Backtest Analytics | 6J Watcher",
    page_icon="📊",
    layout="wide",
)
st.title("📊 6J Watcher — Backtest Analytics (8 meses)")

# ── Sidebar: controles ───────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### ⚙️ Configurações")
    db_path = st.text_input("Caminho do DB", value=DEFAULT_DB)
    min_occ = st.slider("Min. ocorrências (hotspots)", 3, 20, 5)
    auto_refresh = st.checkbox("Auto-refresh (60s)", value=True)
    if auto_refresh:
        st_autorefresh(interval=60_000, key="bt_refresh")
    st.markdown("---")
    st.caption("🔒 Modo read-only — não interfere no orquestrador.")

# ── Carrega dados ─────────────────────────────────────────────────────────────
summary    = load_summary(db_path)
sig_dist   = load_signature_distribution(db_path)
session_bk = load_session_breakdown(db_path)
hotspots   = load_hotspots_historical(db_path, min_occurrences=min_occ)
monthly    = load_monthly_progress(db_path)
profile    = load_calibrated_profile()
hourly_heat= load_hourly_heatmap(db_path)
cumdelta   = load_cumdelta_by_level(db_path)

# ── Erro de conexão ───────────────────────────────────────────────────────────
if "error" in summary:
    st.error(
        f"❌ Não foi possível conectar ao DB: `{summary['error']}`\n\n"
        "Verifique se o caminho está correto ou se o backtest já iniciou."
    )
    st.stop()

# ════════════════════════════════════════════════════════════════════════════
# BLOCO 1 — Métricas de Progresso
# ════════════════════════════════════════════════════════════════════════════
st.markdown("## 📈 Progresso do Backtest")

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total Clusters",      f"{summary['total_clusters']:,}")
c2.metric("Tape Events",         f"{summary['total_tape_events']:,}")
c3.metric("Níveis Únicos",       f"{summary['unique_levels']:,}")
c4.metric("Dias Processados",    f"{summary['days_processed']}")
c5.metric("Período",             f"{summary['first_event'][:10]} → {summary['last_event'][:10]}"
          if summary['first_event'] != "—" else "—")

st.markdown("---")

# ════════════════════════════════════════════════════════════════════════════
# PAINEL A — "Onde as whales defendem?" (Zonas Institucionais)
# ════════════════════════════════════════════════════════════════════════════
st.markdown(f"## 🎯 Painel A: Onde as whales defendem? (Top Hotspots Normalizados)")

if hotspots and "error" not in hotspots[0]:
    df_hs = pd.DataFrame(hotspots)
    df_hs["price"]      = df_hs["price"].apply(lambda x: f"{x:.5f}")
    df_hs["imbalance"]  = df_hs.apply(
        lambda r: f"+{int(r['total_bid'] - r['total_ask']):,}"
        if r["total_bid"] >= r["total_ask"]
        else f"{int(r['total_bid'] - r['total_ask']):,}",
        axis=1,
    )

    st.dataframe(
        df_hs[[
            "price", "occurrences", "dominant_signature",
            "imbalance", "active_days",
        ]].rename(columns={
            "price":               "Nível (Normalizado)",
            "occurrences":         "Toques/Defesas",
            "dominant_signature":  "Assinatura Dominante",
            "imbalance":           "Imbalance Médio",
            "active_days":         "Dias Ativos",
        }),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info(f"Nenhum hotspot com ≥{min_occ} ocorrências ainda.")

st.markdown("---")

# ════════════════════════════════════════════════════════════════════════════
# PAINEL B — "Absorção funcionou? Taxa de reversão"
# ════════════════════════════════════════════════════════════════════════════
st.markdown("## 🧬 Painel B: Eficácia da Absorção (Win Rate)")

if "signatures" in profile:
    sigs = profile["signatures"]
    
    # Processar dados do profile
    data = []
    for key, stats in sigs.items():
        # key é no formato "assinatura_SESSAO"
        parts = key.rsplit("_", 1)
        if len(parts) == 2:
            sig, sess = parts
            data.append({
                "Assinatura": sig,
                "Sessão": sess,
                "Amostras": stats["count"],
                "Win Rate (%)": f"{stats['win_rate'] * 100:.1f}%",
                "Profit Factor": stats["profit_factor"],
                "MFE Médio": f"{stats['avg_mfe']:.5f}"
            })
            
    if data:
        df_profile = pd.DataFrame(data).sort_values(by="Amostras", ascending=False)
        st.dataframe(df_profile, use_container_width=True, hide_index=True)
    else:
        st.info("Perfil calibrado não contém assinaturas ainda.")
elif "error" in profile:
    st.warning(profile["error"])
else:
    st.info("Aguardando calibração empírica...")

st.markdown("---")

# ════════════════════════════════════════════════════════════════════════════
# PAINEL C — Pressão acumulada por nível
# ════════════════════════════════════════════════════════════════════════════
st.markdown("## 🧲 Painel C: Pressão Acumulada por Nível (Cumdelta)")

if cumdelta and "error" not in cumdelta[0]:
    df_cd = pd.DataFrame(cumdelta)
    df_cd["price"] = df_cd["price"].astype(str)
    
    st.bar_chart(
        df_cd.set_index("price")["cumdelta_total"],
        use_container_width=True,
        color="#00C896"
    )
else:
    st.info("Aguardando dados de cumdelta...")

st.markdown("---")

# ════════════════════════════════════════════════════════════════════════════
# PAINEL D — Relógio Institucional (Heatmap Hora × Assinatura)
# ════════════════════════════════════════════════════════════════════════════
st.markdown("## 🕐 Painel D: Relógio Institucional (Horários de Pico)")

if hourly_heat and "error" not in hourly_heat[0]:
    df_hh = pd.DataFrame(hourly_heat)
    
    # Pivotar para formar um heatmap real (linhas=Hora, colunas=Assinaturas)
    df_pivot = df_hh.pivot(index="hour_utc", columns="signature", values="count").fillna(0)
    
    st.dataframe(
        df_pivot.style.background_gradient(cmap="viridis", axis=None),
        use_container_width=True
    )
else:
    st.info("Aguardando dados horários...")

st.caption(
    f"🗄️ DB: `{db_path}` | 🔒 read-only | "
    f"🔄 Refresh: {'60s automático' if auto_refresh else 'manual'}"
)
