-- =============================================================================
-- HOP #2 — CONTAR EVENTOS POR JANELA
-- =============================================================================
--
-- Objetivo:
--
--   Contar quantos eventos participam de cada janela HOP.
--
-- Diferentemente da query anterior, agora existe agregação.
--
-- O Flink precisa manter STATE para cada janela ativa.
--
-- Exemplo:
--
--   [10:00:00, 10:00:15) -> 14 eventos
--   [10:00:05, 10:00:20) -> 18 eventos
--   [10:00:10, 10:00:25) -> 21 eventos
--
--
-- IMPORTANTE:
--
-- Como as janelas se sobrepõem, um mesmo evento pode contribuir
-- para o COUNT de várias janelas.
--
-- Portanto:
--
--   soma dos COUNTs das janelas
--
-- não representa necessariamente:
--
--   quantidade de eventos distintos do stream.
--
--
-- Pergunta respondida:
--
--   "Quantos eventos existem dentro de cada horizonte móvel
--    de 15 segundos, recalculado a cada 5 segundos?"
--
-- =============================================================================

SELECT
    window_start,
    window_end,
    COUNT(*) AS quantidade_eventos
FROM TABLE(
    HOP(
        TABLE autorizacao,
        DESCRIPTOR(event_ts),
        INTERVAL '5' SECOND,
        INTERVAL '15' SECOND
    )
)
GROUP BY
    window_start,
    window_end;