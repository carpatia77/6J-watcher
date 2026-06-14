# Fase 3 — Bloco A: Protocolo de Validação Out-Of-Sample

> *"In God we trust. All others must bring data."*
> — W. Edwards Deming (atribuído a Jim Simons como lema operacional da Renaissance)

---

## 1. Enquadramento Epistemológico

A Fase 2 produziu uma **Lei Empírica In-Sample (IS)**: a convergência entre agulhadas do Didi Index e eventos de Spoofing Pull no L2 atinge 55.9% sob o critério Standard (≥100L ±30min) no trimestre Q4 2025.

Mas uma lei IS não vale um cêntimo se não sobreviver ao OOS. A história das finanças quantitativas está repleta de cadáveres de estratégias que brilhavam no backtest e colapsavam no forward. O nosso trabalho agora é submeter esta hipótese a **sete testes draconianos**, qualquer um dos quais tem o poder de a destruir.

**Filosofia de Validação:** Não estamos a tentar provar que o modelo funciona. Estamos a tentar destruí-lo de todas as formas possíveis. Se ele sobreviver, acreditamos nele. Se falhar num único teste crítico, descartamos ou recalibramos antes de avançar.

---

## 2. Arquitectura de Dados

### 2.1 Separação Absoluta IS / OOS

| Bloco | Período | Base de Dados | Finalidade |
|:------|:--------|:--------------|:-----------|
| **In-Sample (IS)** | Out–Dez 2025 | `backtest_2025_train.db` | Calibração e descoberta |
| **Out-Of-Sample (OOS)** | Jan–Jun 2026 | `backtest_2026_oos.db` | Validação cega |

> [!CAUTION]
> **Regra de Ouro (Renaissance):** Nenhum parâmetro da Fase 2 (thresholds de 100L/200L, janelas de ±15m/±30m, filtros de `MIN_SNAPSHOTS`, `CANCEL_TRADE_RATIO`) pode ser modificado durante a análise OOS. Se tocarmos num único parâmetro baseado nos resultados OOS, contaminamos a validação e o teste perde toda a validade. Os parâmetros entram **congelados** e saem intactos.

### 2.2 Pipeline de Ingestão OOS

O script `run_backtest_historical.py` já foi configurado para processar 6 chunks mensais (Jan–Jun 2026) num banco isolado. A ingestão será idêntica ao processo IS — mesma engine, mesmos filtros, mesma granularidade de 60s por batch. **Zero graus de liberdade adicionais.**

---

## 3. Bateria de Testes Estatísticos

### Teste 1: Teste de Permutação (Null Distribution Empírica)

**Objetivo:** Construir a distribuição nula da convergência por acaso puro, sem depender de premissas paramétricas (normalidade, independência).

**Método:**
1. Extrair todas as `N_oos` agulhadas detectadas no período OOS.
2. Extrair todos os timestamps de eventos Spoofing Pull ≥100L no período OOS.
3. Para cada iteração `k ∈ {1, ..., 10.000}`:
   - Baralhar aleatoriamente os timestamps das agulhadas (mantendo os spoofings fixos).
   - Calcular a convergência ±30min entre as agulhadas permutadas e os spoofings reais.
   - Registar `C_k` = taxa de convergência da permutação `k`.
4. Construir o histograma de `{C_1, ..., C_10000}` = **Distribuição Nula Empírica**.
5. Calcular o **p-value empírico**: `p = (# permutações com C_k ≥ C_obs) / 10.000`.

**Critério PASS:** `p < 0.01` (rejeição da hipótese nula a 99% de confiança).

**Porque é decisivo:** Este teste não assume nenhuma distribuição teórica. Ele constrói a realidade alternativa de um universo onde agulhadas e spoofings não têm relação causal, e mede se o nosso resultado observado é extraordinário nesse universo.

---

### Teste 2: Bootstrap Block — Intervalo de Confiança a 95%

**Objetivo:** Quantificar a incerteza estatística da convergência OOS, respeitando a autocorrelação temporal dos dados financeiros.

**Método:**
1. Dividir as `N_oos` agulhadas em blocos temporais contíguos de **5 e 10 dias úteis** (1 e 2 semanas de mercado, para englobar a autocorrelação de 3-7 dias do Didi).
2. Para cada iteração `b ∈ {1, ..., 5.000}`:
   - Reamostrar blocos **com reposição** (Block Bootstrap, preservando a estrutura temporal intra-bloco).
   - Recalcular a convergência Standard para a amostra bootstrap.
3. Ordenar os 5.000 valores e extrair os percentis 2.5% e 97.5% → **IC 95%**.

**Critério PASS:** O limite inferior do IC 95% deve ser ≥ 40% **em ambas as janelas de bloco**. Se o bloco=10 falhar mas o bloco=5 passar, indica que o modelo sofre de fragmentação artificial e falha ao manter a convergência durante o ciclo completo de uma trend (1-2 semanas).

**Porque é decisivo:** O bootstrap de blocos (e não o bootstrap ingénuo) respeita o facto de que agulhadas em dias consecutivos podem estar correlacionadas (tendência/momentum de mercado). Isto evita subestimar a variância.

---

### Teste 3: Teste Z de Duas Proporções (IS vs. OOS)

**Objetivo:** Testar formalmente se a convergência OOS é **estatisticamente indistinguível** da convergência IS. Não queremos que ela seja "melhor" (sinal de overfitting invertido) — queremos que seja **estável**.

**Método:**
- `p_IS = 19/34 = 0.559`  (convergência In-Sample)
- `p_OOS` = convergência observada OOS
- `n_IS = 34`, `n_OOS` = número de agulhadas OOS
- Estatística de teste: `Z = (p_IS - p_OOS) / sqrt(p_pool × (1 - p_pool) × (1/n_IS + 1/n_OOS))`
  - onde `p_pool = (x_IS + x_OOS) / (n_IS + n_OOS)`

**Critério PASS:** `|Z| < 1.96` (não rejeitamos H₀ de igualdade a α=0.05). Ou seja, IS e OOS são **estatisticamente iguais**. 

**Interpretação dos cenários:**
- `|Z| < 1.96` → **PASS** — Estabilidade confirmada. O modelo generaliza.
- `Z > 1.96` (IS >> OOS) → **FAIL** — Decaimento severo. Possível overfitting.
- `Z < -1.96` (OOS >> IS) → **WARN** — Convergência OOS suspeitamente alta. Investigar se houve regime de mercado excepcionalmente favorável.

---

### Teste 4: Correção de Múltiplas Hipóteses (Benjamini-Hochberg FDR)

**Objetivo:** Corrigir o facto de que na Fase 2 testámos **múltiplas combinações** de parâmetros antes de chegar ao resultado final. Cada combinação é um "trial", e sem correção, a probabilidade de encontrar um falso positivo cresce geometricamente.

**Inventário de Trials (Fase 2):**

| # | Threshold | Janela | Trial |
|:--|:----------|:-------|:------|
| 1 | ≥38L | ±15min | Descartado (base rate) |
| 2 | ≥100L | ±15min | Giant |
| 3 | ≥150L | ±15min | Testado |
| 4 | ≥200L | ±15min | Titanium |
| 5 | ≥100L | ±30min | **Standard (eleito)** |
| 6 | ≥200L | ±30min | Testado implicitamente |
| 7 | ≥38L | ±30min | Threshold original, janela expandida |
| 8 | ≥38L | ±60min | Janela original que gerou 100% |
| 9 | ≥150L | ±30min | Intermédio testado |
| 10| ≥200L | ±60min | Case study dos 336L |

**Método:**
1. Para cada um dos **10 trials** (m=10), calcular o p-value OOS via Teste de Permutação (Teste 1).
2. Ordenar os p-values: `p_(1) ≤ p_(2) ≤ ... ≤ p_(10)`.
3. Aplicar a correção Benjamini-Hochberg: rejeitar H₀ para todos `p_(i) ≤ (i/10) × 0.05`.

**Critério PASS:** O p-value corrigido do sinal Standard (≥100L ±30min) deve permanecer significativo (`q < 0.05`) após a correção FDR.

**Porque é decisivo:** Este é o teste que o Marcos López de Prado (ex-AQR, autor de *Advances in Financial Machine Learning*) considera obrigatório. Sem ele, somos culpados de *Selection Bias under Multiple Testing* (SBuMT) — o pecado capital do backtesting quantitativo.

---

### Teste 5: Walk-Forward Mensal (Estabilidade Temporal)

**Objetivo:** Verificar se a convergência é temporalmente estável ou se é um artefacto de 1-2 meses excepcionais mascarando meses mortos.

**Método:**
Para cada mês `m ∈ {Jan, Fev, Mar, Abr, Mai, Jun}`:
1. Extrair as agulhadas e spoofings exclusivos do mês `m`.
2. Calcular `C_m` = convergência Standard do mês.
3. Registar `N_m` = número de agulhadas do mês.

**Saída:** Tabela + gráfico de barras mensal.

| Mês | Agulhadas | Convergência | Confiança |
|:----|:----------|:-------------|:----------|
| Jan/26 | ? | ?% | IC bootstrap |
| Fev/26 | ? | ?% | IC bootstrap |
| ... | ... | ... | ... |

**Critério PASS:** 
1. Pelo menos 4 dos 6 meses devem ter convergência ≥ 40%.
2. **Nenhum bloco de 2 meses consecutivos** pode ter convergência média < 30%. (Falhar nisto significa colapso estrutural, impossibilitando o trading contínuo).

**Porque é decisivo:** A Renaissance descarta modelos que funcionam "em média" mas falham catastroficamente em períodos específicos. Um modelo robusto não tem buracos negros temporais.

---

### Teste 6: Análise de Decaimento (Alpha Decay)

**Objetivo:** Medir se o sinal perde força à medida que nos afastamos do período de calibração. Se a convergência caiu monotonicamente de Jan→Jun, o modelo está a morrer — mesmo que a média OOS ainda passe no gate.

**Método:**
1. Usar a série temporal do Teste 5 (`C_jan, C_fev, ..., C_jun`).
2. Ajustar uma regressão linear: `C_m = β₀ + β₁ × m + ε`.
3. Testar a significância de `β₁` (coeficiente de inclinação temporal).

**Critério PASS:** Decaimento máximo aceitável é de `-3pp por mês` (`DECAY_THRESHOLD = -0.03`). O coeficiente `β₁` deve ser absoluto: se `β₁ < -0.05` (-5pp por mês), o teste é **FAIL** independentemente do p-value (visto que n=6 retira poder estatístico).

**Cenários:**
- `β₁ ≥ 0` → Sinal estável ou a fortalecer. Ideal.
- `-0.03 ≤ β₁ < 0` → Tendência de queda aceitável, não compromete a esperança matemática no curto prazo.
- `β₁ < -0.05` → **FAIL absoluto**. Perda de 25pp entre Jan e Jun. O alfa expirou.

---

### Teste 7: Análise Condicional de Regime (Robustez Estrutural)

**Objetivo:** Verificar se o sinal é robusto a diferentes regimes de mercado, ou se funciona apenas em condições específicas.

**Dimensões de Segmentação:**

| Dimensão | Segmentos |
|:---------|:----------|
| **Sessão** | Asian / London / New York |
| **Volatilidade** | Alta (VIX > 20 ou ATR > mediana) vs. Baixa |
| **Regime BOJ** | Alta Ingerência (ATR_diário > 2× mediana OOS) vs. Normal. Fundamental para o JPY. |
| **Dia da Semana** | Seg-Ter-Qua-Qui-Sex |

**Método:**
Para cada segmento:
1. Filtrar agulhadas e spoofings pertencentes ao segmento.
2. Calcular convergência segmentada.
3. Comparar com a convergência global via teste de Fisher exact (tabela 2×2).

**Critério PASS:** A convergência não pode ser **exclusivamente** dependente de uma única sessão ou regime. Se 90% da convergência vem apenas da sessão Asian, o modelo é frágil.

---

## 4. Matriz de Decisão Final

> [!IMPORTANT]
> **Veredicto Final: Regras de Aprovação/Rejeição**

| Resultado | Condição | Acção |
|:----------|:---------|:------|
| **APROVADO** | ≥ 5 dos 7 testes PASS, incluindo obrigatoriamente os Testes 1, 3 e 4 | Avançar para Bloco B/C (Produção) |
| **CALIBRAR** | 3-4 testes PASS, sem falha nos Testes 1 e 4 | Expandir OOS para Jul-Set 2026 e repetir |
| **REJEITADO AUTOMÁTICO** | Teste 5 mostrar 2 meses consecutivos com Convergência = 0% | Aborto letal. Sinal evapora em regimes desconhecidos. |
| **REJEITADO** | < 3 testes PASS, ou falha no Teste 1 (Permutação) | Descartar hipótese L2. Pivotar para Didi puro sem DOM |

Os Testes 1 (Permutação) e 4 (FDR) são **eliminatórios** porque testam a validade fundamental do sinal. Sem eles, qualquer resultado positivo é potencialmente um artefacto estatístico.

---

## 5. Entregáveis

| Artefacto | Descrição |
|:----------|:----------|
| `backtest_2026_oos.db` | Base de dados isolada com 6 meses de clusters OOS |
| `oos_permutation_test.py` | Script do Teste de Permutação (10k iterações) |
| `oos_statistical_battery.py` | Script consolidado dos 7 testes |
| `phase3_oos_report.md` | Relatório final com tabelas, gráficos e veredicto |

---

## 6. Plano de Execução

### Etapa 1: Ingestão de Dados (Computação Pesada)
- Rodar `run_backtest_historical.py` com 6 chunks mensais (Jan–Jun 2026).
- Tempo estimado: ~9-12h no Pentium Gold (1.5-2h/mês).
- **Requer autorização explícita do operador para iniciar.**

### Etapa 2: Extração de Agulhadas OOS
- Gerar OHLCV horário a partir do `backtest_2026_oos.db`.
- Calcular Didi Index e detectar agulhadas com os mesmos parâmetros congelados da Fase 2.

### Etapa 3: Bateria de Testes
- Executar os 7 testes em sequência.
- Gerar tabelas e visualizações para cada teste.

### Etapa 4: Relatório e Veredicto
- Compilar `phase3_oos_report.md` com todos os resultados.
- Emitir veredicto binário: **APROVADO** ou **REJEITADO**.

---

## Open Questions

> [!NOTE]
> **VERIFICAÇÃO DE CACHE DATABENTO (CONCLUÍDO ✅)**
> Foi verificado localmente que o cache no servidor já contém os 5 ficheiros ZST correspondentes ao período de 01/01/2026 a 05/06/2026. **Zero custo Databento** previsto para a Validação OOS.

> [!WARNING]
> **Sobre o Tamanho da Amostra OOS**
> Para o Teste Z ter poder estatístico adequado (≥ 80%) de detectar uma queda de 15pp (55% → 40%) com α=0.05, exigimos `n_OOS ≥ 52 agulhadas`.
> IS: 34 agulhadas em 3 meses (11.3/mês). OOS (Jan-Jun) projeta ~68 agulhadas, o que garante poder estatístico.
> Se porventura a extração final devolver < 50 agulhadas, seremos forçados a estender o dataset (Jul-Set) para restaurar a integridade paramétrica do teste.
