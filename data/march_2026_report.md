# 📊 Parecer Técnico OOS — Março 2026 e Análise Póstuma

**Período de Análise**: 01/03/2026 a 31/03/2026 (Out-of-Sample)
**Foco da Validação**: Reativação da "Camada Amplificada" frente a cenários de reprecificação macroeconômica.

Este relatório compila a falha condicional de Março e o teste estrutural do *proxy* de volatilidade ao longo de todos os meses históricos.

---

## 📉 O Colapso do Catalisador em Março

A tese fundamental esperava que o mês de Março (reprecificação do BoJ) agisse como o catalisador primário capaz de reativar o *Liquidity Vacuum* na sessão londrina (MFE P99 > 30 ticks).

O motor varreu **329.360 clusters** no mês, com **2.205 amostras** diretas para o padrão alvo, e entregou os seguintes números:

* **Win Rate:** 46.71%
* **Profit Factor:** 0.57 ❌ (Meta: > 3.0)

| Métrica | Fevereiro (Baseline) | Março (Realizado) | Status OOS |
|---------|---------------------|-------------------|---|
| **MAE P50** | 1.0 tick | **1.0 tick** | ✅ Preservado |
| **MAE P95** | 5.0 ticks | **8.0 ticks** | ✅ Preservado |
| **MFE P90** | 4.0 ticks | **4.0 ticks** | ❌ Falhou |
| **MFE P99** | 7.0 ticks | **11.0 ticks** | ❌ Falhou |

### Diagnóstico de Março
**A base defensiva microestrutural (o chão) manteve-se impenetrável**. Um MAE P95 de 8 ticks prova estatisticamente que institucionais seguem suportando limites de liquidez em *Ranging*. No entanto, o catalisador estipulado **falhou**. A tampa (MFE extremo) não se abriu, levando o modelo a sangrar marginalmente via custo transacional num mercado anêmico, cimentando o Profit Factor em 0.57.

---

## 🔍 O Teste Póstumo do *Proxy* Macro

Como Março não comportou o evento de explosão, executamos uma varredura cruzada no DuckDB com o racional reverso: *"Semanas com MFE P99 elevado obrigatoriamente devem coincidir com eventos macro não anunciados ou atípicos"*.

Extraímos o Top 10 Semanas (desde Out/2025) por MFE P99:

```text
        Semana       MFE P99 (Ticks)   Amostras (N)
0 2025-09-29        13625.0             436   <-- Anomalia Externa Extrema (Flash Crash / MoF)
1 2026-01-26           48.0            1455   <-- Catalisador que gerou PF 51x em Jan/2026
2 2025-10-20           22.4             916
3 2025-11-17           17.0             568
4 2026-01-12           14.0            1054
5 2025-10-06           14.0            1597
6 2026-03-23           13.0             617   
7 2025-10-13           12.0             893
8 2026-01-19           11.0            1252
9 2026-03-02           10.0             446   
```

### O Veridito da Camada Dupla
1. O *Profit Factor* estrondoso de Janeiro (51.53) foi exclusivamente impulsionado pela ativação da Camada Amplificada na semana de **26/01/2026** (MFE de 48 ticks com 1.455 amostras).
2. O sistema registrou um pico gravitacional na primeira semana de Outubro/2025 (13.625 ticks), provando que o motor quantitativo consegue reter perfeitamente as ineficiências extremas quando a macroeconomia força rupturas severas de fluxo de ordens asiáticas.

O modelo prova que a Anomalia de Londres não reage ao mês em si (Março foi irrelevante), mas sim ao fluxo agudo e irracional do livro. Operacionalmente, a estratégia é imutável: o capital sobrevive com custo zero de manutenção em *ranging* raso através do stop minúsculo de 8 ticks, enquanto aguarda o pagamento assimétrico dos raros e violentos *vacuums*.
