-- =============================================================================
-- HOP #6 — VISUALIZAR OS EVENTOS QUE COMPÕEM CADA JANELA
-- =============================================================================
--
-- Objetivo:
--
--   Produzir uma linha por janela contendo:
--
--      - quantidade de eventos;
--      - ARRAY com os event_id da janela.
--
--
-- Exemplo:
--
--   [10:00:00,10:00:15)
--       -> [e1,e2,e3,e4]
--
--   [10:00:05,10:00:20)
--       -> [e2,e3,e4,e5,e6]
--
--   [10:00:10,10:00:25)
--       -> [e3,e4,e5,e6,e7]
--
--
-- Essa visualização torna a SOBREPOSIÇÃO do HOP bastante clara.
--
-- Observe que:
--
--   e2
--   e3
--   e4
--
-- aparecem em mais de uma janela.
--
--
-- A query é STATEFUL porque o Flink precisa manter os eventos
-- pertencentes às janelas ainda ativas.
--
-- Pergunta respondida:
--
--   "Quais eventos pertencem a cada janela HOP?"
--
-- =============================================================================

SELECT
    window_start,
    window_end,

    COUNT(*) AS quantidade_eventos,

    ARRAY_AGG(event_id) AS eventos

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