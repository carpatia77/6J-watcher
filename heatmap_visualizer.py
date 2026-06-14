"""
heatmap_visualizer.py
---------------------
Gera um heatmap térmico 2D do Order Book (DOM L2/MBP-10)
ao redor de um evento de Spoofing detectado.

Estratégia de seek (Opção B):
  - Lê o .dbn.zst do cache local do Databento diretamente
  - Descarta records ANTES da janela com overhead mínimo (sem processar levels)
  - Captura o DOM completo apenas dentro da janela ±window_s ao redor do target
  - Zero bytes de disco adicional (não persiste nada)

Uso:
  python heatmap_visualizer.py --ts 1761022571508939649 --window 3 --out spoofing_event.html
  python heatmap_visualizer.py --top5   # Gera os 5 maiores spoofing_bid_pull do banco IS

Requisitos:
  pip install plotly pandas databento
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import duckdb
import databento as db

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    _PLOTLY_OK = True
except ImportError:
    _PLOTLY_OK = False

try:
    import pandas as pd
    _PANDAS_OK = True
except ImportError:
    _PANDAS_OK = False

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────
CACHE_DIR    = Path("/home/aidea/data_backtest/databento")
DB_PATH      = "/home/aidea/data_backtest/backtest_2025_train.db"
TICK_SIZE    = 0.0000005   # 6J: 0.5 pips = $6.25
PRICE_SCALE  = 1_000_000_000

# Paleta de cores
COLOR_BID_MAX   = "rgb(0, 150, 255)"   # Azul intenso = muralha de compra forte
COLOR_BID_MED   = "rgb(100, 200, 255)" # Azul claro
COLOR_ASK_MAX   = "rgb(255, 80, 0)"    # Laranja intenso = muralha de venda forte
COLOR_ASK_MED   = "rgb(255, 160, 100)" # Laranja claro
COLOR_CANCEL    = "rgb(220, 0, 80)"    # Vermelho = cancelamento (puxada)
COLOR_TRADE     = "rgb(0, 220, 80)"    # Verde = agressão (trade executado)


# ─────────────────────────────────────────────────────────────────────────────
# Busca os Top N eventos de spoofing no banco IS
# ─────────────────────────────────────────────────────────────────────────────
def query_top_spoofing_events(
    db_path: str,
    sig: str = "spoofing_bid_pull",
    n: int = 5,
) -> list[dict]:
    """Retorna os N maiores eventos de spoofing por volume cancelado."""
    conn = duckdb.connect(db_path, read_only=True)
    rows = conn.execute(f"""
        SELECT
            timestamp_ns,
            timestamp,
            price,
            confidence,
            json_extract_string(raw_payload, '$.cancel_bid_vol')::INT as cancel_bid_vol,
            json_extract_string(raw_payload, '$.cancel_ask_vol')::INT as cancel_ask_vol
        FROM liquidity_clusters
        WHERE behavior_signature = '{sig}'
        ORDER BY cancel_bid_vol DESC
        LIMIT {n}
    """).fetchall()
    conn.close()
    return [
        {
            "timestamp_ns":    r[0],
            "timestamp":       r[1],
            "price":           r[2],
            "confidence":      r[3],
            "cancel_bid_vol":  r[4],
            "cancel_ask_vol":  r[5],
        }
        for r in rows
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Encontra o arquivo .dbn.zst certo para um timestamp_ns
# ─────────────────────────────────────────────────────────────────────────────
def find_cache_file(target_ts_ns: int, cache_dir: Path) -> Optional[Path]:
    """Encontra o arquivo de cache que contém o timestamp alvo."""
    target_dt = datetime.fromtimestamp(target_ts_ns / 1e9, tz=timezone.utc)
    target_date = target_dt.date()

    for f in sorted(cache_dir.glob("*.dbn.zst")):
        # Extrair datas do nome: symbol_YYYY-MM-DD_YYYY-MM-DD_schema.dbn.zst
        parts = f.stem.split("_")
        # Ex: ['6J.n.0', '2025-10-05', '2025-10-31', 'mbp-10']
        try:
            from datetime import date
            start_str = parts[-3]
            end_str   = parts[-2]
            start_d   = date.fromisoformat(start_str)
            end_d     = date.fromisoformat(end_str)
            if start_d <= target_date <= end_d:
                return f
        except (ValueError, IndexError):
            continue
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Seek sequencial + extração de DOM na janela alvo
# ─────────────────────────────────────────────────────────────────────────────
def extract_dom_window(
    file_path: Path,
    target_ts_ns: int,
    window_s: float = 2.0,
    max_levels: int = 6,
) -> Tuple[List[dict], List[dict]]:
    """
    Extrai snapshots de DOM e trades na janela [target - window_s, target + window_s].

    Estratégia: leitura sequencial com fast-forward.
    - Antes da janela: lê apenas ts_event (sem processar levels) → overhead mínimo
    - Na janela: processa DOM completo e trades
    - Depois da janela: break imediato

    Returns:
        dom_snapshots: list de {ts_ns, price, bid_vol, ask_vol, level}
        tape_events:   list de {ts_ns, price, size, side}
    """
    window_ns   = int(window_s * 1e9)
    win_start   = target_ts_ns - window_ns
    win_end     = target_ts_ns + window_ns

    dom_snapshots: List[dict] = []
    tape_events:   List[dict] = []

    records_skipped = 0
    records_read    = 0

    logger.info(f"Seek em {file_path.name} para ts={target_ts_ns}")
    logger.info(f"Janela: {win_start} → {win_end} ({window_s*2:.1f}s)")

    store = db.DBNStore.from_file(str(file_path))
    for record in store:
        ts = record.ts_event

        # Fast-forward: ainda antes da janela
        if ts < win_start:
            records_skipped += 1
            continue

        # Passou da janela → para
        if ts > win_end:
            break

        records_read += 1

        # Capture trades (action=T)
        action = getattr(record, "action", None)
        action_val = getattr(action, "value", action)
        if action_val in ("T", "84", 84):
            size = getattr(record, "size", 0)
            if size > 0:
                side_raw  = str(getattr(record, "side", "N"))
                side_char = side_raw.split(".")[-1].strip().upper()[0]
                side      = "buy" if side_char == "B" else ("sell" if side_char == "A" else None)
                if side:
                    tape_events.append({
                        "ts_ns": ts,
                        "price": record.price / PRICE_SCALE,
                        "size":  size,
                        "side":  side,
                    })

        # Capture DOM snapshots (MBP-10 tem .levels)
        if hasattr(record, "levels") and record.levels:
            for i, lv in enumerate(record.levels[:max_levels]):
                if lv.bid_sz > 0:
                    dom_snapshots.append({
                        "ts_ns":   ts,
                        "price":   lv.bid_px / PRICE_SCALE,
                        "bid_vol": lv.bid_sz,
                        "ask_vol": 0,
                        "level":   i,
                        "side":    "bid",
                    })
                if lv.ask_sz > 0:
                    dom_snapshots.append({
                        "ts_ns":   ts,
                        "price":   lv.ask_px / PRICE_SCALE,
                        "bid_vol": 0,
                        "ask_vol": lv.ask_sz,
                        "level":   i,
                        "side":    "ask",
                    })

    logger.info(f"Skipped: {records_skipped:,} | In-window: {records_read:,}")
    logger.info(f"DOM snapshots: {len(dom_snapshots)} | Trades: {len(tape_events)}")
    return dom_snapshots, tape_events



# ─────────────────────────────────────────────────────────────────────────────
# Construção do heatmap Plotly
# ─────────────────────────────────────────────────────────────────────────────
def build_heatmap(
    dom_snapshots: List[dict],
    tape_events:   List[dict],
    target_ts_ns:  int,
    event_meta:    dict,
    out_path:      str = "spoofing_heatmap.html",
):
    """
    Cria e salva o heatmap 2D interativo (Plotly) do Order Book.

    Layout:
      - Painel principal: BID heatmap (azul) + ASK heatmap (laranja) sobrepostos
      - Linha vertical vermelha = instante do evento de spoofing detectado
      - Marcadores verdes = trades executados na janela
      - Título com metadata do evento
    """
    if not _PLOTLY_OK or not _PANDAS_OK:
        raise ImportError("plotly e pandas são necessários: pip install plotly pandas")

    df = pd.DataFrame(dom_snapshots)
    if df.empty:
        logger.error("Sem dados de DOM para plotar")
        return

    target_dt = datetime.fromtimestamp(target_ts_ns / 1e9, tz=timezone.utc)

    # Eixo X: tempo em ms relativo ao evento
    df["t_ms"]      = (df["ts_ns"] - target_ts_ns) / 1e6
    df["price_str"] = df["price"].apply(lambda p: f"{p:.6f}")

    # Separar bid e ask
    bid_df = df[df["side"] == "bid"].copy()
    ask_df = df[df["side"] == "ask"].copy()

    # Pivotar para matriz: linhas = preços únicos, colunas = snapshots de tempo
    def pivot_side(sdf: pd.DataFrame, vol_col: str) -> pd.DataFrame:
        """Agrega volume por (t_ms_bin, price) para criar a matriz de calor."""
        sdf = sdf.copy()
        # Binarizar t_ms em bins de 50ms para criar uma matriz navegável
        sdf["t_bin"] = (sdf["t_ms"] // 50) * 50
        pivot = sdf.groupby(["t_bin", "price_str"])[vol_col].sum().unstack(fill_value=0)
        return pivot

    bid_pivot = pivot_side(bid_df, "bid_vol")
    ask_pivot = pivot_side(ask_df, "ask_vol")

    # Todos os preços únicos (eixo Y unificado)
    all_prices = sorted(
        set(bid_pivot.columns.tolist()) | set(ask_pivot.columns.tolist()),
        reverse=True  # preço mais alto no topo
    )
    all_times = sorted(
        set(bid_pivot.index.tolist()) | set(ask_pivot.index.tolist())
    )

    def to_matrix(pivot: pd.DataFrame) -> List[List[float]]:
        """Converte pivot para matriz [preço × tempo], preenchendo ausentes com 0."""
        mat = []
        for price in all_prices:
            row = []
            for t in all_times:
                val = pivot.at[t, price] if (t in pivot.index and price in pivot.columns) else 0
                row.append(float(val))
            mat.append(row)
        return mat

    bid_matrix = to_matrix(bid_pivot)
    ask_matrix = to_matrix(ask_pivot)

    # ── Build Plotly figure ──────────────────────────────────────────────────
    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.75, 0.25],
        subplot_titles=["Order Book Heatmap (DOM L2)", "Trade Tape"],
        vertical_spacing=0.08,
    )

    # BID heatmap (azul)
    fig.add_trace(
        go.Heatmap(
            z=bid_matrix,
            x=[f"{t:.0f}ms" for t in all_times],
            y=all_prices,
            colorscale=[[0, "rgba(0,0,0,0)"], [0.1, "rgb(100,180,255)"], [1, "rgb(0,100,255)"]],
            showscale=True,
            colorbar=dict(title="BID vol", x=1.02, len=0.5),
            name="BID",
            hovertemplate="Tempo: %{x}<br>Preço: %{y}<br>Vol BID: %{z}<extra></extra>",
        ),
        row=1, col=1
    )

    # ASK heatmap (laranja/vermelho)
    fig.add_trace(
        go.Heatmap(
            z=ask_matrix,
            x=[f"{t:.0f}ms" for t in all_times],
            y=all_prices,
            colorscale=[[0, "rgba(0,0,0,0)"], [0.1, "rgb(255,180,100)"], [1, "rgb(255,60,0)"]],
            showscale=True,
            colorbar=dict(title="ASK vol", x=1.10, len=0.5),
            name="ASK",
            hovertemplate="Tempo: %{x}<br>Preço: %{y}<br>Vol ASK: %{z}<extra></extra>",
        ),
        row=1, col=1
    )

    # Linha vertical no instante t=0 (evento detectado)
    # Nota: add_vline não funciona com eixo categórico — usamos add_shape com
    # coordenadas de papel (xref='paper') para pintar a linha no centro da janela.
    # O "centro" visual = índice do bin t=0 entre all_times
    t0_labels = [f"{t:.0f}ms" for t in all_times]
    center_label = "0ms"
    if center_label in t0_labels:
        center_frac = t0_labels.index(center_label) / max(len(t0_labels) - 1, 1)
    else:
        center_frac = 0.5  # fallback: meio da janela

    fig.add_shape(
        type="line",
        xref="paper", yref="paper",
        x0=center_frac, x1=center_frac,
        y0=0.25, y1=1.0,   # ocupa apenas o painel superior (row=1)
        line=dict(color=COLOR_CANCEL, width=2, dash="dash"),
    )
    fig.add_annotation(
        xref="paper", yref="paper",
        x=center_frac, y=1.01,
        text=f"Spoofing Detectado<br>{target_dt.strftime('%H:%M:%S.%f')}",
        showarrow=False,
        font=dict(size=9, color=COLOR_CANCEL),
        bgcolor="rgba(22,27,34,0.8)",
        bordercolor=COLOR_CANCEL,
        borderwidth=1,
    )

    # Trades na janela
    if tape_events:
        tape_df = pd.DataFrame(tape_events)
        tape_df["t_ms"] = (tape_df["ts_ns"] - target_ts_ns) / 1e6
        tape_df["t_label"] = tape_df["t_ms"].apply(lambda x: f"{x:.0f}ms")
        tape_df["price_str"] = tape_df["price"].apply(lambda p: f"{p:.6f}")

        for side, color, sym in [("buy", COLOR_TRADE, "triangle-up"), ("sell", COLOR_CANCEL, "triangle-down")]:
            side_df = tape_df[tape_df["side"] == side]
            if not side_df.empty:
                fig.add_trace(
                    go.Scatter(
                        x=side_df["t_label"],
                        y=side_df["price_str"],
                        mode="markers",
                        marker=dict(symbol=sym, size=10, color=color, line=dict(width=1, color="white")),
                        name=f"Trade {side.upper()}",
                        hovertemplate=f"Trade {side.upper()}<br>Tempo: %{{x}}<br>Preço: %{{y}}<br>Size: %{{customdata}}<extra></extra>",
                        customdata=side_df["size"],
                    ),
                    row=1, col=1
                )

        # Painel de tape (volume por lado)
        tape_df["t_bin"] = (tape_df["t_ms"] // 50) * 50
        tape_agg = tape_df.groupby(["t_bin", "side"])["size"].sum().unstack(fill_value=0)
        t_labels = [f"{t:.0f}ms" for t in tape_agg.index]

        if "buy" in tape_agg.columns:
            fig.add_trace(
                go.Bar(x=t_labels, y=tape_agg["buy"], name="BUY vol",
                       marker_color=COLOR_TRADE, opacity=0.8),
                row=2, col=1
            )
        if "sell" in tape_agg.columns:
            fig.add_trace(
                go.Bar(x=t_labels, y=-tape_agg["sell"], name="SELL vol",
                       marker_color=COLOR_CANCEL, opacity=0.8),
                row=2, col=1
            )

    # ── Layout ───────────────────────────────────────────────────────────────
    fig.update_layout(
        title=dict(
            text=(
                f"<b>6J Spoofing Pull — {target_dt.strftime('%Y-%m-%d %H:%M:%S.%f')} UTC</b><br>"
                f"<sub>Preço: {event_meta.get('price', 'N/A'):.6f} | "
                f"Conf: {event_meta.get('confidence', 0)*100:.1f}% | "
                f"Cancel BID: {event_meta.get('cancel_bid_vol', 0):,} lotes | "
                f"Cancel ASK: {event_meta.get('cancel_ask_vol', 0):,} lotes</sub>"
            ),
            font=dict(size=14),
        ),
        paper_bgcolor="#0d1117",
        plot_bgcolor="#161b22",
        font=dict(color="#e6edf3", size=11),
        barmode="relative",
        height=800,
        showlegend=True,
        legend=dict(
            bgcolor="rgba(22,27,34,0.8)",
            bordercolor="#30363d",
            borderwidth=1,
        ),
        hovermode="x unified",
    )

    fig.update_xaxes(showgrid=True, gridcolor="#21262d", zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="#21262d", zeroline=False)

    # Salvar
    fig.write_html(out_path, include_plotlyjs="cdn")
    logger.info(f"Heatmap salvo: {out_path}")
    print(f"\n✅ Heatmap gerado: {out_path}")
    print(f"   DOM rows plotados: {len(dom_snapshots)}")
    print(f"   Trades na janela:  {len(tape_events)}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )

    parser = argparse.ArgumentParser(description="6J Order Book Heatmap Visualizer")
    parser.add_argument("--ts",     type=int,   help="Timestamp alvo em nanosegundos (timestamp_ns do cluster)")
    parser.add_argument("--window", type=float, default=2.0, help="Janela em segundos antes/depois do evento (default=2.0)")
    parser.add_argument("--top5",   action="store_true", help="Gerar heatmaps dos 5 maiores spoofing_bid_pull")
    parser.add_argument("--sig",    type=str,   default="spoofing_bid_pull", help="Assinatura para --top5 (default=spoofing_bid_pull)")
    parser.add_argument("--out",    type=str,   default=None, help="Arquivo de saída HTML")
    parser.add_argument("--db",     type=str,   default=DB_PATH, help="Caminho do banco DuckDB IS")
    parser.add_argument("--cache",  type=str,   default=str(CACHE_DIR), help="Diretório de cache .dbn.zst")
    parser.add_argument("--levels", type=int,   default=6, help="Número de níveis do DOM a exibir (default=6)")
    args = parser.parse_args()

    cache_dir = Path(args.cache)
    out_dir   = Path("/mnt/c/Users/aidea/.gemini/6J-watcher/output/heatmaps")
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.top5:
        events = query_top_spoofing_events(args.db, sig=args.sig, n=5)
        print(f"\n📊 Top 5 eventos de '{args.sig}':")
        for i, ev in enumerate(events, 1):
            print(f"  {i}. {ev['timestamp']} | cancel_bid={ev['cancel_bid_vol']:,} | conf={ev['confidence']*100:.1f}%")
        print()

        for i, ev in enumerate(events, 1):
            ts_ns    = ev["timestamp_ns"]
            out_file = str(out_dir / f"heatmap_{args.sig}_{i:02d}_{ts_ns}.html")
            print(f"Gerando heatmap {i}/5: ts={ts_ns}")

            cache_file = find_cache_file(ts_ns, cache_dir)
            if not cache_file:
                logger.warning(f"  Cache não encontrado para ts={ts_ns}")
                continue

            dom_snaps, trades = extract_dom_window(
                cache_file, ts_ns, window_s=args.window, max_levels=args.levels
            )
            if dom_snaps:
                build_heatmap(dom_snaps, trades, ts_ns, ev, out_path=out_file)
            else:
                print(f"  ⚠️  Sem dados DOM na janela de {args.window}s")

    elif args.ts:
        ts_ns      = args.ts
        out_file   = args.out or str(out_dir / f"heatmap_{ts_ns}.html")
        cache_file = find_cache_file(ts_ns, cache_dir)

        if not cache_file:
            print(f"ERRO: nenhum arquivo .dbn.zst encontrado para ts={ts_ns}")
            sys.exit(1)

        print(f"Arquivo: {cache_file}")
        dom_snaps, trades = extract_dom_window(
            cache_file, ts_ns, window_s=args.window, max_levels=args.levels
        )

        if dom_snaps:
            build_heatmap(dom_snaps, trades, ts_ns, {"price": 0.0, "confidence": 1.0}, out_path=out_file)
        else:
            print(f"⚠️  Sem dados DOM na janela de ±{args.window}s")

    else:
        parser.print_help()
        print("\nExemplo: python heatmap_visualizer.py --top5")
        print("         python heatmap_visualizer.py --ts 1761022571508939649")


if __name__ == "__main__":
    main()
