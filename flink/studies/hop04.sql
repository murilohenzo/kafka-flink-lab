-- =============================================================================
-- HOP #4 — FILTRAR TRANSAÇÕES DE VALOR >= 50 E AGRUPAR POR STATUS
-- =============================================================================
--
-- Objetivo:
--
--   Contar eventos cujo valor seja >= 50.00,
--   separados pelo status de autorização,
--   em janelas móveis de 15 segundos.
--
--
-- Ordem lógica:
--
--   evento
--      |
--      v
--   HOP
--      |
--      v
--   WHERE valor >= 50
--      |
--      v
--   GROUP BY status + janela
--      |
--      v
--   COUNT
--
--
-- O WHERE determina quais registros participam da agregação.
--
-- Pergunta respondida:
--
--   "Quantas autorizações com valor >= 50 ocorreram por status
--    nos últimos 15 segundos, produzindo uma nova janela
--    a cada 5 segundos?"
--
-- =============================================================================

SELECT
    status_autorizacao,
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
WHERE valor >= 50.00
GROUP BY
    status_autorizacao,
    window_start,
    window_end;