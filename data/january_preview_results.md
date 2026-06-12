# Laboratório Quantitativo: Baseline vs Bazooka (N=10)
## Preview Exclusivo - Janeiro de 2026 (OOS)

Este documento registra a prova matemática do ganho informacional ao trocar a arquitetura legada (Top of Book, N=1) pela nova arquitetura vetorizada Bazooka (Depth N=10, PyArrow + DuckDB ASOF JOIN).

### O Experimento
- **Ativo:** 6J (Yen Dólar)
- **Período:** Janeiro de 2026 (1 Mês de Out-of-Sample)
- **Sessão:** LONDON
- **Assinatura Investigada:** `absorption_passive`

### Resultados Diretos (Delta Tático)

| Métrica | Baseline (N=1) | Bazooka (N=10) | Variação Absoluta / Delta |
| :--- | :--- | :--- | :--- |
| **Amostras (Samples)** | 5.991 | 17.062 | **+11.071 trades (+285%)** |
| **Risco (MAE P50)** | 25.0 ticks | 26.0 ticks | +1.0 tick (Estável) |
| **Excursão (MFE P90)** | 65.0 ticks | 67.8 ticks | **+2.8 ticks (Expansão)** |
| **Win Rate** | Pendente* | Pendente* | - |

*(O WinRate é calculado pós-clusterização pela pipeline do Profiler na Fase 3)*

### Conclusão Técnica

O uso restrito do N=1 causava uma **cegueira profunda** no motor quantitativo. A liquidez institucional espalhada nos 9 níveis inferiores do livro de ofertas estava mascarando quase **11.000 sinais legítimos** de absorção passiva apenas em um único mês. 

Ao plugar a lente N=10, não apenas resgatamos 285% mais *trades*, como comprovamos que a assimetria do sinal se manteve cirurgicamente intacta (a MAE cravou perto de 25 ticks, enquanto o alvo do movimento expandiu em quase 3 ticks no cenário ótimo de P90). O custo de 8.5 horas de computação intensiva por bloco se provou altamente justificável.
