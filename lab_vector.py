import time
import pandas as pd
import numpy as np


# ---------------------------------------------------------------------------
# Parâmetros de janelamento — devem coincidir com batch_size_seconds do runner
# ---------------------------------------------------------------------------
WINDOW_NS = 250_000_000   # 250 ms em nanosegundos (ajuste conforme config)
N_SYNTHETIC = 10_000      # volume realista para medir throughput


def _make_synthetic_tape(n: int, window_ns: int) -> list:
    """Gera n eventos sintéticos com timestamp_ns crescente e side aleatório."""
    rng = np.random.default_rng(42)
    base_ns = 1_700_000_000_000_000_000   # epoch realista (Nov 2023)
    timestamps = base_ns + np.cumsum(rng.integers(1_000_000, 5_000_000, size=n))
    prices     = 10.0 + np.cumsum(rng.uniform(-0.05, 0.05, size=n))
    volumes    = rng.integers(1, 20, size=n).astype(int)
    sides      = rng.choice(["buy", "sell"], size=n)
    return [
        {"timestamp_ns": int(timestamps[i]), "price": float(prices[i]),
         "volume": int(volumes[i]), "side": str(sides[i])}
        for i in range(n)
    ]


def sandbox_vector_test():
    print("=== Sandbox: Teste de Vetorizacao do Pipeline ===")

    # ── 5 eventos manuais para validação visual ──────────────────────────────
    raw_tape_small = [
        {"timestamp_ns": 100_000_000, "price": 10.0, "volume": 5,  "side": "buy"},
        {"timestamp_ns": 200_000_000, "price": 10.5, "volume": 10, "side": "buy"},
        {"timestamp_ns": 350_000_000, "price": 11.0, "volume": 3,  "side": "sell"},
        {"timestamp_ns": 400_000_000, "price": 10.0, "volume": 7,  "side": "sell"},
        {"timestamp_ns": 450_000_000, "price": 9.5,  "volume": 2,  "side": "buy"},
    ]

    print(f"\n[1] Validacao visual ({len(raw_tape_small)} eventos):")
    clusters_small = _vectorize(raw_tape_small, WINDOW_NS)
    print(clusters_small.to_string())

    # ── Dataset realista para medir throughput ───────────────────────────────
    # Warm-up do Pandas antes de iniciar o timer (primeiro uso inicializa JIT interno)
    _vectorize(raw_tape_small, WINDOW_NS)

    raw_tape_big = _make_synthetic_tape(N_SYNTHETIC, WINDOW_NS)
    print(f"\n[2] Throughput ({N_SYNTHETIC:,} eventos, epoch realista):")
    t0 = time.perf_counter()
    clusters_big = _vectorize(raw_tape_big, WINDOW_NS)
    t_elapsed = time.perf_counter() - t0

    print(f"  Clusters gerados : {len(clusters_big)}")
    print(f"  Tempo            : {t_elapsed*1000:.2f}ms")
    print(f"  Throughput       : {N_SYNTHETIC / t_elapsed:,.0f} eventos/s")
    print()
    print("  NOTE: cumdelta_min/max acima deve ser validado contra")
    print("  _build_clusters_sql() em ingestion.py antes de usar como")
    print("  referência de teste — o campo 'delta' do SQL usa signed_vol")
    print("  calculado por running_delta (SUM OVER window), não cumsum().")


def _vectorize(raw_tape: list, window_ns: int) -> pd.DataFrame:
    """
    Vetoriza uma lista de tape-events em clusters agregados por janela.

    Parâmetros
    ----------
    raw_tape   : List[Dict] com chaves timestamp_ns, price, volume, side
    window_ns  : tamanho da janela em nanosegundos

    BUG1 FIX: window_id calculado com delta relativo ao primeiro timestamp
    do batch (não epoch absoluto) — garante janelas corretas em dados reais.
    """
    df = pd.DataFrame(raw_tape)

    # BUG1 FIX: t0_ns relativo ao batch, não epoch absoluto
    t0_ns = int(df["timestamp_ns"].iloc[0])
    df["window_id"] = (df["timestamp_ns"] - t0_ns) // window_ns

    # Sinal de volume: buy=+ / sell=-
    df["signed_vol"] = np.where(
        df["side"].str.lower().isin(["buy", "b"]),
        df["volume"], -df["volume"]
    )
    df["bid_vol"] = np.where(df["signed_vol"] > 0, df["volume"], 0)
    df["ask_vol"] = np.where(df["signed_vol"] < 0, df["volume"], 0)

    # Cumdelta intra-janela (CVD incremental)
    df["cumdelta"] = df.groupby("window_id")["signed_vol"].cumsum()

    agg_funcs = {
        "timestamp_ns": "first",
        "price":        ["first", "last"],
        "bid_vol":      "sum",
        "ask_vol":      "sum",
        "cumdelta":     ["min", "max", "last"],
    }
    clusters = df.groupby("window_id").agg(agg_funcs)
    clusters.columns = ["_".join(col).strip() for col in clusters.columns.values]
    clusters["delta_price"] = clusters["price_last"] - clusters["price_first"]

    return clusters


if __name__ == "__main__":
    sandbox_vector_test()
