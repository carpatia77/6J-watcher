# 🏛️ Arquitetura Unificada: A Síntese Definitiva do Sistema 6J

A validação OOS (Out-Of-Sample) de Janeiro a Abril de 2026 expôs a verdadeira natureza do sistema quantitativo, levando à obsolescência do modelo original de "Duas Camadas" e o substituindo pelo **Modelo Unificado**.

## 1. O Problema da Camada 1 (Microestrutura) Isolada
Os testes contínuos de `absorption_passive_RANGING_LONDON` sem amostragem revelaram que, operada de forma cega, a Camada 1 sangra lentamente via custo transacional:
* **Fev, Mar, Abr**: PF entre 0.57 e 0.94.
* **Jan**: PF de 0.75 (embora tenha capturado um evento MFE extremo de 37 ticks).

O excesso de ocorrências (4.500+ amostras/mês) afoga o "Bilhete Dourado" no ruído. **A Camada 1 não é lucrativa operada sozinha.** Ela fornece um chão intransponível (MAE P95 cravado em 5 a 10 ticks incondicionalmente), mas precisa de um gatilho direcional estrito.

## 2. A Camada 2 (Didi Index) como o Target
O estudo do Didi Index (Agulhada Score=6) atua exatamente preenchendo a falha da Camada 1. Isoladamente, a Camada 2 já entrega métricas assustadoras (WR 79%, R:R 11.2, 251 pips em 7 dias). 

A conexão ocorre porque o Didi isola o *timing* da exaustão macro. Ele detecta a pré-condição do movimento amplificado.

## 3. O Fluxo Unificado
A arquitetura de *Alpha* do robô passa a ser sequencial:

1. **O Motor Macro (Didi Index)** busca as Agulhadas perfeitas nos tempos gráficos institucionais (H1/H4). Ele age como o farol.
2. **A Pré-Condição:** Uma vez que o Didi pisca, o algoritmo aguarda a sessão de Londres entrar em `RANGING`.
3. **O Gatilho Micro (Camada 1):** O motor de *Order Flow* desce para o *Tape* (nível de carrapato) procurando a assinatura `absorption_passive`. 

**Por que isso funciona?** 
Porque a absorção passiva valida no livro de ofertas que os institucionais de fato entraram na direção da agulhada macro. O robô não entra de peito aberto no Didi; ele entra no nível do tick exato onde o institucional absorveu o varejo. Se a agulhada for falsa, o gatilho micro não dispara. Se for real, o *stop-loss* estrutural de 8 ticks provido pela Camada 1 protegerá o capital, enquanto o alvo macro da agulhada pagará 40 a 80 ticks.

Isso isola o MFE extremo e elimina as 4.539 ocorrências que eram ruído transacional.
