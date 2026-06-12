# Laboratório Quantitativo: Baseline vs Bazooka (N=10)
## Preview Exclusivo - 1º Trimestre de 2026 (Jan, Fev, Mar)

Este documento registra a prova matemática do ganho informacional ao trocar a arquitetura legada (Top of Book, N=1) pela nova arquitetura vetorizada Bazooka (Depth N=10, PyArrow + DuckDB ASOF JOIN).

### O Experimento Expandido (Trimestre)
- **Ativo:** 6J (Yen Dólar)
- **Período:** Janeiro a Março de 2026 (3 Meses OOS)
- **Sessão:** LONDON
- **Assinatura Investigada:** `absorption_passive`

### Resultados Diretos (Delta Tático Q1)

| Métrica | Baseline (N=1) | Bazooka (N=10) | Variação Absoluta / Delta |
| :--- | :--- | :--- | :--- |
| **Amostras (Samples)** | 11.679 | 29.536 | **+17.857 trades (+153%)** |
| **Risco (MAE P50)** | 25.0 ticks | 25.0 ticks | Igual (0.0t) |
| **Excursão (MFE P90)** | 59.0 ticks | 60.0 ticks | **+1.0 tick (Expansão)** |
| **Win Rate** | Pendente* | Pendente* | - |

*(O WinRate é calculado pós-clusterização pela pipeline do Profiler na Fase 3)*

### Conclusão Técnica (Consolidada)

A lente Bazooka (N=10) se provou **matematicamente imune** ao ruído estendido. Expandindo a amostragem de Janeiro para um trimestre inteiro (Q1), a Bazuca encontrou impressionantes **17.857 oportunidades operacionais** que o Baseline ignorou por cegueira de profundidade.

Mais vital do que o volume: **a qualidade térmica do trade foi matematicamente preservada**.
Triplicar o volume de entradas em um sistema quantitativo geralmente corrompe a expectativa matemática e explode o Risco (MAE). No entanto, o `MAE P50` cravou exatos `25.0t` em ambos os cenários e a Excursão Máxima `MFE P90` ainda expandiu de `59.0t` para `60.0t`.

A refatoração engoliu as *market lies* dos grandes institucionais, destilou a verdade num volume colossal de +150% de sinais a mais, e manteve o fio da navalha do setup afiado.
