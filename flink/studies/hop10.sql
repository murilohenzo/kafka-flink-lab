-- =============================================================================
-- HOP #10 — PRESERVANDO WINDOW_TIME PARA OUTRA OPERAÇÃO TEMPORAL
-- =============================================================================
--
-- Objetivo:
--
--   Realizar uma Window Aggregation e preservar um TIME ATTRIBUTE
--   que possa ser utilizado em outra etapa temporal posteriormente.
--
--
-- window_start e window_end:
--
--   descrevem os limites da janela,
--   porém tornam-se timestamps comuns após a agregação.
--
-- window_time:
--
--   pode preservar a propriedade temporal da janela quando incluído
--   como chave do GROUP BY.
--
--
-- Isso é útil para:
--
--   - cascading windows;
--   - outra Window TVF;
--   - operações temporais posteriores;
--   - Window Top-N;
--   - determinados joins temporais.
--
-- =============================================================================

SELECT
    status_autorizacao,

    window_start,
    window_end,

    -- Time attribute propagado pela agregação.
    window_time,

    COUNT(*) AS quantidade,

    SUM(valor) AS valor_total

FROM TABLE(
    HOP(
        TABLE autorizacao,
        DESCRIPTOR(event_ts),
        INTERVAL '5' SECOND,
        INTERVAL '15' SECOND
    )
)
GROUP BY
    status_autorizacao,
    window_start,
    window_end,
    window_time;