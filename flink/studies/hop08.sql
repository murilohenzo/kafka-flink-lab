-- =============================================================================
-- HOP #8 — SUA QUERY ORIGINAL: CONTAGEM POR EVENT_ID
-- =============================================================================
--
-- Objetivo real desta consulta:
--
--   Contar quantas ocorrências do MESMO event_id aparecem dentro
--   de cada janela HOP.
--
--
-- ATENÇÃO:
--
-- Como event_id normalmente representa um evento único, incluir:
--
--   event_id
--
-- no GROUP BY faz com que o COUNT(*) normalmente seja:
--
--   1
--
-- Exemplo:
--
--   event_id = EVT-A
--
--   [10:00:00,10:00:15) -> 1
--   [10:00:05,10:00:20) -> 1
--   [10:00:10,10:00:25) -> 1
--
-- Isso NÃO significa que o evento ocorreu três vezes.
--
-- Significa que o mesmo evento foi atribuído a três janelas HOP
-- diferentes devido à sobreposição.
--
--
-- COUNT(*) > 1 para a MESMA combinação:
--
--   event_id
--   window_start
--   window_end
--
-- pode ser interessante para investigação de duplicidade.
--
--
-- `window_time` está presente no GROUP BY para preservar o atributo
-- temporal da janela caso esse resultado seja utilizado posteriormente
-- em outra operação temporal.
--
-- =============================================================================

SELECT
    event_id,
    status_autorizacao,
    window_start,
    window_end,
    window_time,
    COUNT(*) AS quantidade
FROM TABLE(
    HOP(
        TABLE autorizacao,

        -- Event Time / event_ts.
        DESCRIPTOR(event_ts),

        -- Uma nova janela é iniciada a cada 5 segundos.
        INTERVAL '5' SECOND,

        -- Cada janela observa um intervalo de 15 segundos.
        INTERVAL '15' SECOND
    )
)

WHERE valor >= 50.00

GROUP BY
    event_id,
    status_autorizacao,
    window_start,
    window_end,
    window_time;