# Auditoria: narrator.py + main.py

Este documento registra como Architecture Decision Record (ADR) a auditoria do módulo `narrator.py` (última milha do pipeline 6J Watcher) e correções relacionadas no `main.py`.

## Contexto

O `narrator.py` é responsável por transformar os dados da `LiquidityMatrix` e do `DuckDBRepository` em saídas legíveis pelo trader: relatório diário em Markdown, alertas em tempo real e sumários de nível de preço. É consumido pelo servidor HTTP em `main.py` nos endpoints `GET /report` e `GET /hotspots`.

---

## Auditoria Iteração 1

### Estado Inicial
- `narrator.py` era um módulo puramente estático sem logging, sem tratamento de tipos de dados dinâmicos e sem validação de contratos de entrada.
- `main.py` instanciava o `AdaptivePatternEngine` passando `tick_size` como argumento explícito, contrariando a centralização via `Config` implementada na Iteração 2 do Engine.

---

## Correções Aplicadas

### Correção 1: TypeError Silencioso em `level_summary()` (`narrator.py`)
- **Problema:** `p.get('price', '?')` retornava `str` quando o campo `price` estava ausente no dicionário. A aplicação do format spec `:.5f` sobre uma string lançava `TypeError` em runtime, silenciado pelo handler HTTP do `main.py`.
- **Ação:** Extraído o campo `price` separadamente com verificação de tipo (`isinstance(price, (int, float))`). A formatação `:.5f` agora só é aplicada se o valor for numérico; caso contrário, exibe `"?"` como fallback seguro.

### Correção 2: Logging Ausente (`narrator.py`)
- **Ação:** Adicionado `import logging` e instanciado `logger = logging.getLogger(__name__)`.
- **Motivo:** Módulo de "última milha" sem logging é opaco a falhas de serialização ou dados malformados oriundos da Matrix/Repository. Com o logger configurado, falhas podem ser rastreadas em produção sem depender de exceções borbulhando até o handler HTTP.

### Correção 3: Argumento Redundante `tick_size` (`main.py`)
- **Ação:** Removido `tick_size=cfg.tick_size` da instanciação do `AdaptivePatternEngine` em `main.py`.
- **Motivo:** Desde a Iteração 2 do Engine (`refactor: centraliza tick_size a partir do config.py`), o construtor do `AdaptivePatternEngine` já lê `tick_size` diretamente do objeto `Config` via `self.cfg.tick_size`. Passar o argumento explicitamente era redundante e abria possibilidade de dessincronismo caso o `cfg.tick_size` fosse sobrescrito entre a instanciação do `cfg` e do `engine`.

---

## Status Final
- `narrator.py`: ✅ Gold Tier
- `main.py` (integração): ✅ Gold Tier
