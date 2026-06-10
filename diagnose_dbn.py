"""
diagnose_dbn.py
---------------
Inspeciona os primeiros N records de um arquivo .dbn.zst e imprime
os campos raw de cada record para diagnosticar tipos de action/side.

Uso:
    python diagnose_dbn.py data/databento/6J.n.0_2025-10-05_2025-10-31_mbp-10.dbn.zst
"""
from __future__ import annotations
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pathlib import Path
import databento as db

ACTION_TRADE = 84  # ord('T')
N_INSPECT    = 2000  # quantos records inspecionar
N_PRINT      = 5     # quantos de cada tipo imprimir


def diagnose(file_path: str):
    path = Path(file_path)
    if not path.exists():
        print(f"Arquivo nao encontrado: {path}")
        sys.exit(1)

    print(f"Arquivo : {path.name}")
    print(f"Lendo primeiros {N_INSPECT} records...\n")

    data = db.DBNStore.from_file(str(path))

    counts = {"total": 0, "trades": 0, "buy": 0, "sell": 0, "neutral": 0, "no_levels": 0}
    trade_samples  = []
    other_samples  = []
    side_values    = set()
    action_values  = set()

    for record in data:
        counts["total"] += 1
        if counts["total"] > N_INSPECT:
            break

        action     = getattr(record, "action", None)
        action_val = getattr(action, "value", action)
        action_values.add((type(action).__name__, repr(action)))

        side     = getattr(record, "side", None)
        side_val = str(side).upper() if side is not None else "NONE"
        side_values.add((type(side).__name__, repr(side), side_val))

        has_levels = hasattr(record, "levels") and record.levels
        if not has_levels:
            counts["no_levels"] += 1

        is_trade = (action_val == ACTION_TRADE)

        if is_trade:
            counts["trades"] += 1
            # Detectar side real
            side_char = side_val.split(".")[-1].strip()  # lida com "Action.B" e "B"
            if side_char == "B":
                counts["buy"] += 1
            elif side_char == "A":
                counts["sell"] += 1
            else:
                counts["neutral"] += 1

            if len(trade_samples) < N_PRINT:
                trade_samples.append({
                    "action_type":  type(action).__name__,
                    "action_repr":  repr(action),
                    "action_value": action_val,
                    "side_type":    type(side).__name__,
                    "side_repr":    repr(side),
                    "side_str":     str(side),
                    "side_val":     side_val,
                    "price":        getattr(record, "price", None),
                    "size":         getattr(record, "size", None),
                    "ts_event":     getattr(record, "ts_event", None),
                })
        else:
            if len(other_samples) < N_PRINT:
                other_samples.append({
                    "action_type":  type(action).__name__,
                    "action_repr":  repr(action),
                    "action_value": action_val,
                })

    # ── Relatório ──
    print("=" * 60)
    print("CONTAGENS")
    print("=" * 60)
    for k, v in counts.items():
        print(f"  {k:<15}: {v:>10,}")

    print()
    print("=" * 60)
    print("TIPOS DE ACTION OBSERVADOS")
    print("=" * 60)
    for type_name, repr_val in sorted(action_values):
        print(f"  type={type_name:<20}  repr={repr_val}")

    print()
    print("=" * 60)
    print("TIPOS DE SIDE OBSERVADOS")
    print("=" * 60)
    for type_name, repr_val, str_val in sorted(side_values):
        print(f"  type={type_name:<20}  repr={repr_val:<30}  str()={str_val}")

    print()
    if trade_samples:
        print("=" * 60)
        print(f"AMOSTRAS DE TRADES ({len(trade_samples)} primeiros)")
        print("=" * 60)
        for i, s in enumerate(trade_samples):
            print(f"  [{i}] action_type={s['action_type']} action_repr={s['action_repr']} "
                  f"action_value={s['action_value']}")
            print(f"       side_type={s['side_type']} side_repr={s['side_repr']} "
                  f"side_str={s['side_str']} side_val={s['side_val']}")
            print(f"       price={s['price']} size={s['size']} ts={s['ts_event']}")
            print()
    else:
        print(f"NENHUM TRADE encontrado nos primeiros {N_INSPECT} records!")
        print("Amostras de outros records:")
        for i, s in enumerate(other_samples):
            print(f"  [{i}] action_type={s['action_type']} repr={s['action_repr']} value={s['action_value']}")

    print()
    print("=" * 60)
    print("DIAGNOSTICO")
    print("=" * 60)
    if counts["trades"] == 0:
        print("  [CRITICO] Nenhum trade detectado com action_val == 84.")
        print("  Verifique os tipos de action acima.")
        print("  Se action e um enum, tente: action_val = action.value")
        print("  Se action e string (ex: 'T'), use: action_val == 'T'")
    elif counts["neutral"] > 0 and counts["buy"] == 0 and counts["sell"] == 0:
        print(f"  [CRITICO] {counts['neutral']} trades com side neutro/desconhecido.")
        print("  Todos os trades foram descartados por side != 'B' e != 'A'.")
        print("  Verifique os tipos de side acima.")
        print("  Se side e enum (ex: Side.BID), use: str(side).split('.')[-1]")
    elif counts["neutral"] > counts["buy"] + counts["sell"]:
        print(f"  [AVISO] Maioria dos trades com side neutro ({counts['neutral']}).")
        print("  Verifique parsing do side.")
    else:
        print(f"  [OK] Trades detectados: {counts['trades']} "
              f"(buy={counts['buy']}, sell={counts['sell']}, neutral={counts['neutral']})")
        print("  extract_tape_event() deveria estar funcionando.")
        print("  Verifique se ingest_batch() esta recebendo tape_rows corretamente.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python diagnose_dbn.py <caminho_para_.dbn.zst>")
        sys.exit(1)
    diagnose(sys.argv[1])
