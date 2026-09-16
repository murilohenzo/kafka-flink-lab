-- =============================================================================
-- HOP #7 — EVENTOS POR TRANSAÇÃO PIX DENTRO DA JANELA
-- =============================================================================
--
-- Objetivo:
--
--   Contar quantos eventos relacionados à mesma `id_transacao_pix`
--   estão presentes dentro de cada janela HOP.
--
--
-- State lógico:
--
--   (id_transacao_pix, window_start, window_end)
--
--
-- Essa consulta é mais orientada à chave de negócio do que
-- uma agregação por event_id.
--
--
-- Exemplo:
--
--   PIX-A
--
--   Window [10:00:00,10:00:15)
--       -> 2 eventos
--
--   Window [10:00:05,10:00:20)
--       -> 3 eventos
--
--
-- Pergunta respondida:
--
--   "Quantos eventos relacionados à mesma transação PIX
--    aparecem em cada horizonte móvel de 15 segundos?"
--
-- =============================================================================

SELECT
    id_transacao_pix,
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
    id_transacao_pix,
    window_start,
    window_end;