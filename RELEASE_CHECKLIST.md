# Checklist de Release (End-to-End)

## Verificações Iniciais (1-8)
[ ] 1. Verificar ausência de erros de compilação no `mql_bridge.mq5`
[ ] 2. Confirmar que a DLL ClusterDelta está sendo carregada corretamente no MetaTrader
[ ] 3. Rodar `test_duckdb.py` e verificar integridade do schema
[ ] 4. Rodar `test_profiler_sql.py` e verificar agregações MFE/MAE
[ ] 5. Rodar `test_bridge.py` e verificar ingestão sintética (status 200)
[ ] 6. Conferir geração de JSONs (`JsonEscape`, `JsonNumber`, `JoinJsonArray`)
[ ] 7. Verificar se `last_closed_price` é hidratado corretamente a partir do DuckDB
[ ] 8. Testar roteamento do `main.py` (`/ingest`, `/hotspots`, `/report`)

## Verificações Avançadas (Dinâmica Institucional e Agendador)
[ ] 9.1  Aguardar 30s após boot e verificar log:
         → "prune_stale_data executado — N clusters removidos"
         (pode ser 0 no cold start — isso é esperado)

[ ] 9.2  Aguardar 30min e verificar log:
         → "profile.json recalibrado — thresholds atualizados para sessão X"
         → engine.profile em memória deve ter timestamp mais recente que o boot

[ ] 9.3a Simular lock conflict do DuckDB durante recalibração:
         → Log deve mostrar: "[Profiler] Lock detectado, retry em 60s"
[ ] 9.3b Sistema continua respondendo GET /hotspots durante o lock simulado
[ ] 9.3c Profile é recalibrado com sucesso no ciclo seguinte após lock liberar

[ ] 9.4  Verificar geração do daily report às 22h UTC:
         SELECT date, LENGTH(report_text) FROM daily_reports ORDER BY date DESC LIMIT 1
         → report_text deve ter > 500 chars (relatório real, não vazio)

[ ] 9.5  Testar idempotência do upsert:
         → Forçar dois ciclos de 22h (ajustar relógio ou mock) e verificar:
            SELECT COUNT(*) FROM daily_reports WHERE date = TODAY → deve ser 1, não 2

[ ] 9.6  Verificar que cumdelta dinâmico muda distribuição de win_rate no profile:
         → Comparar profile.json antes e depois da correção
         → ICEBERG_ACCUMULATION deve ter win_rate diferente do anterior
