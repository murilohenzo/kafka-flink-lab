-- =============================================================================
-- HOP #9 — PROCURAR EVENT_ID DUPLICADO DENTRO DA MESMA JANELA
-- =============================================================================
--
-- Objetivo:
--
--   Transformar a query anterior em uma consulta de diagnóstico
--   de possíveis eventos duplicados.
--
--
-- Primeiro:
--
--   GROUP BY event_id + janela
--
-- Depois:
--
--   HAVING COUNT(*) > 1
--
--
-- Importante:
--
-- O mesmo event_id estar presente em TRÊS JANELAS DIFERENTES
-- NÃO representa duplicidade.
--
-- Isso é comportamento normal do HOP.
--
--
-- Duplicidade relevante para esta query seria:
--
--   event_id = EVT-A
--
-- dentro da MESMA:
--
--   window_start
--   window_end
--
-- aparecer mais de uma vez.
--
--
-- Pergunta respondida:
--
--   "Existe algum event_id repetido dentro da mesma janela HOP?"
--
-- =============================================================================

SELECT
    event_id,
    window_start,
    window_end,
    COUNT(*) AS quantidade
FROM TABLE(
    HOP(
        TABLE autorizacao,
        DESCRIPTOR(event_ts),
        INTERVAL '5' SECOND,
        INTERVAL '15' SECOND
    )
)
GROUP BY
    event_id,
    window_start,
    window_end

HAVING COUNT(*) > 1;