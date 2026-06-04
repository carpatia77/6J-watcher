# Auditoria e Refatoração: Pattern Engine (V1 → V2)

Este documento registra como um Architecture Decision Record (ADR) a migração do motor de classificação de clusters do sistema 6J Watcher, partindo de uma implementação monolítica estática (V1) para uma arquitetura bi-modular adaptativa (V2).

## Contexto: Por Que o V1 Foi Deprecado

O `pattern_engine.py` original utilizava regras heurísticas com thresholds hardcoded para classificar `LiquidityCluster` em `BehaviorSignature`. Essa abordagem apresentava três falhas estruturais fundamentais:

### Falha #1: Thresholds Estáticos (Cross-Session Failure)
- **Problema:** Valores como `total >= 20` ou `imbalance_ratio >= 0.5` eram absolutos e fixos. O volume médio na sessão Asiática é ordens de magnitude menor que na sessão de Nova York. Um threshold que funciona em NY classifica tudo como `UNKNOWN` na Ásia.
- **Impacto:** Perda massiva de sinais durante sessões de baixa liquidez.

### Falha #2: Falácia Gaussiana (Normalização Inválida)
- **Problema:** Qualquer tentativa de normalizar os volumes via Z-Score (Média + Desvio Padrão) seria matematicamente incorreta. Distribuições de volume em mercados financeiros são leptocúrticas (fat-tailed): spikes de volume como o Non-Farm Payroll ou decisões do FOMC distorcem a média e o desvio padrão, tornando Z-Scores inúteis.
- **Impacto:** Falsos positivos massivos durante eventos de alta volatilidade.

### Falha #3: Ausência de Tick Displacement (ΔP)
- **Problema:** O V1 classificava "Absorção" baseado apenas em volume alto e imbalance baixo. Porém, na microestrutura real, Absorção requer que o preço NÃO se desloque (ΔP ≤ 1 tick) apesar do volume. O V1 não recebia nem processava essa informação.
- **Impacto:** Confusão entre Absorção e simples consolidação de preço.

---

## Arquitetura V2: Sistema Bi-Modular Adaptativo

A solução adotada decompõe o problema em duas responsabilidades completamente desacopladas:

### Módulo A: `signature_profiler.py` (Offline Calibrator)
- **Responsabilidade:** Análise histórica via DuckDB SQL Window Functions. Calcula MFE/MAE (Maximum Favorable/Adverse Excursion) e gera tabelas de percentis empíricos por sessão.
- **Saída:** Arquivo `profile.json` contendo os percentis p50, p75, p90, p95, p99 para volume e imbalance, segmentados por sessão (ASIAN, LONDON, NEW_YORK, OFF_HOURS).
- **Frequência:** Executado periodicamente (ex: diário, pré-pregão) para recalibrar os thresholds com base nos últimos N dias de dados.
- **Princípio:** Rank Normalization (percentis empíricos) em vez de Z-Scores. Imune a outliers.

### Módulo B: `adaptive_pattern_engine.py` (Online Classifier)
- **Responsabilidade:** Classificação em tempo real com latência O(1). Carrega `profile.json` em memória e compara cada cluster contra os percentis da sessão corrente.
- **Parâmetro Obrigatório:** `delta_price_ticks` (ΔP) — número de ticks que o preço se deslocou durante a formação do cluster.
- **Heurísticas de Microestrutura:**
  - `ABSORPTION_PASSIVE`: Volume ≥ p90, Imbalance ≥ p90, |ΔP| ≤ 1 tick
  - `BREAKOUT_GENUINE`: Volume ≥ p75, Imbalance ≥ p75, |ΔP| ≥ 2 ticks
  - `ICEBERG_ACCUMULATION/DISTRIBUTION`: Volume ≥ p75, |ΔP| = 0, Imbalance < p90
  - `DEFENSE_LINE`: 3+ eventos defensivos no mesmo nível (via `post_classify`)
  - `MAGNET_EFFECT`: 3+ toques no mesmo nível de preço (via `post_classify`)

---

## Impacto Residual nos Módulos Existentes

| Módulo | Nível | Alteração |
|---|---|---|
| `ingestion.py` | ALTO | Import atualizado. Loop de criação de clusters agora calcula ΔP e passa para `classify()`. |
| `main.py` | MÉDIO | Instanciação de `AdaptivePatternEngine` com path do profile e tick_size. |
| `liquidity_matrix.py` | BAIXO | Sem alteração necessária (fallback `classify` usa default `delta_price_ticks=0`). |
| `requirements.txt` | BAIXO | Adição de `numpy` e `pandas`. |
| `config.py` | BAIXO | Padronização de sessões para UPPERCASE (pendente aprovação). |

---

*(Este documento será expandido com os registros de cada correção aplicada durante a implementação).*
