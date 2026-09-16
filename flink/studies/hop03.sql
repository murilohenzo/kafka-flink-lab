-- =============================================================================
-- HOP #3 — CONTAR EVENTOS POR STATUS DENTRO DE CADA JANELA
-- =============================================================================
--
-- Objetivo:
--
--   Analisar a distribuição dos status de autorização
--   dentro de cada janela móvel de 15 segundos.
--
-- State lógico:
--
--   (status_autorizacao, window_start, window_end)
--
--
-- Exemplo:
--
--   Window [10:00:00,10:00:15)
--
--       APROVADA  -> 12
--       NEGADA    ->  3
--
--   Window [10:00:05,10:00:20)
--
--       APROVADA  -> 15
--       NEGADA    ->  4
--
--
-- Pergunta respondida:
--
--   "Como a quantidade de autorizações aprovadas/negadas
--    evolui em uma janela móvel de 15 segundos?"
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
GROUP BY
    status_autorizacao,
    window_start,
    window_end;