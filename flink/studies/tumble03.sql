-- ============================================================
-- AGRUPAR OS EVENT_ID QUE PERTENCEM A CADA JANELA
--
-- Objetivo:
--   Produzir uma linha por janela contendo um ARRAY com
--   todos os event_id observados naquela janela.
--
-- Exemplo:
--
-- [10:00:00, 10:00:15) -> [e1, e2, e3, e4]
-- [10:00:15, 10:00:30) -> [e5, e6]
--
-- Esta consulta é uma WINDOW AGGREGATION e, portanto,
-- é STATEFUL.
--
-- ============================================================

SELECT
    window_start,
    window_end,
    COUNT(*) AS quantidade_eventos,
    ARRAY_AGG(event_id) AS eventos
FROM TABLE(
    TUMBLE(
        TABLE autorizacao,
        DESCRIPTOR(event_ts),
        INTERVAL '15' SECOND
    )
)
GROUP BY
    window_start,
    window_end;