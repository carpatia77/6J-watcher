# AUDIT — ingestion.py

> Última revisão: **2026-06-08**  
> Revisado por: Perplexity / carpatia77  
> Commits desta sessão: [`d2223a1`](https://github.com/carpatia77/6J-watcher/commit/d2223a19a11d6d0857acdcc53832ad47a37b6456)

---

## Arquitetura geral

`IngestionService` é o sistema nervoso central do pipeline. Unifica:
1. Parse T&S + DOM (`parser_tsdom.py`)
2. Micro-agregação em janelas de 250ms (`_build_clusters_from_windows`)
3. Classificação comportamental (`AdaptivePatternEngine.classify`)
4. Persistência no DuckDB (`DuckDBRepository`)
5. Atualização da `LiquidityMatrix`
6. Refinamento pós-classificação (`post_classify` a cada 10 batches)

---

## Fixes aplicados — sessão 2026-06-08

### FIX-ING-01 — `import bisect` movido para topo do módulo
**Severidade:** Estilo / performance acumulada  
**Problema:** `import bisect` estava dentro de `_dom_at`, gerando lookup em `sys.modules` a cada chamada. Em 8 meses de backtest (~milhões de janelas), o overhead acumula.  
**Fix:** `import bisect` movido para escopo global no topo do arquivo.  
**Impacto residual:** Nenhum. Módulos que chamam `ingest_batch` não precisam de alteração.

---

### FIX-ING-02 — DOM index: dict → lista ordenada de tuplas
**Severidade:** 🔴 Performance crítica para backtest de 8 meses  
**Problema:** `_build_dom_index` retornava `dict[price_key → {ts_ns: (bid, ask)}]`. O `_dom_at` chamava `sorted(ts_map.keys())` **a cada invocação**, reconstruindo a lista ordenada O(K log K) por janela de 250ms.  
**Fix:**
- `_build_dom_index` agora retorna `dict[price_key → List[Tuple[ts_ns, bid, ask]]]`
- A lista é ordenada **uma única vez** no build (`.sort()` por `ts_ns`)
- `_dom_at` faz busca binária manual O(log K) diretamente sobre a lista pré-ordenada
- Sem `import bisect` necessário no método (já no topo)

**Complexidade:**
```
Antes:  O(K log K) por chamada _dom_at  (K = snapshots DOM por preço)
Agora:  O(log K)  por chamada _dom_at
Build:  O(D log D) uma vez por batch  (D = total dom_rows)
```

**Impacto residual nos módulos dependentes:**
- `backtest_runner.py` — chama `ingest_batch`, sem mudança de interface
- `main.py` (produção) — idem, sem mudança de interface
- `LiquidityMatrix` — não consome `dom_index` diretamente, sem impacto

---

### FIX-ING-03 — `top_n` como parâmetro configurável
**Severidade:** 🟡 Melhoria de cobertura de sinal  
**Problema:** `_build_dom_index` tinha `top_n=5` hardcoded. Para o 6J (CME micro-FX), o Compound Man frequentemente esconde Icebergs nos níveis 6–10 do Book.  
**Fix:** `top_n` exposto como parâmetro em `_build_dom_index` e em `ingest_batch` (default=5).  
**Uso no backtest:** `ingest_batch(..., top_n=10)` — ver `backtest_runner.py`  
**Uso em produção:** `ingest_batch(...)` — mantém `top_n=5` por default (sem regressão)  
**Custo adicional:** Nenhum — O(log K) do bisect lida com K maior sem degradação.

---

## Integridade validada (sem fix necessário)

| Ponto | Status |
|---|---|
| `_last_market_ts = None` reset entre chunks | ✅ Correto — `prune_stale_data` recebe tempo de mercado real |
| `executemany` para UPDATE em lote de clusters upgraded | ✅ Correto — evita catastrophic UPDATE individual |
| `(0, 0)` de `_dom_at` quando preço não encontrado | ✅ Seguro — usado apenas como metadado em `raw_payload`, não como input classificador |
| Sequência parse → index → cluster → persist → refine | ✅ Blindada — `rollback()` no `except` de cada etapa |
| Fallback produção MQL5 sem `timestamp_ns` | ✅ Cada TapeEvent gera sua própria janela sem regressão |

---

## Observação de operações

> `profile.json` gerado antes do commit `d2223a1` (janelas tick-único) **deve ser deletado**  
> antes do primeiro run do backtest. O perfil antigo tem percentis uma ordem de grandeza  
> abaixo da distribuição de janelas de 250ms e causaria classificação degenerada.

```powershell
del data\backtest_profile.json
python run_backtest_historical.py
```
