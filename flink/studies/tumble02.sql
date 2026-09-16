-- =============================================================================
-- TUMBLE #2 — INSPEÇÃO DA ATRIBUIÇÃO DOS EVENTOS ÀS JANELAS
-- =============================================================================
--
-- Objetivo:
--   Visualizar em qual janela de 15 segundos cada evento da tabela
--   `autorizacao` foi colocado pelo Flink.
--
-- Diferentemente da query anterior, esta consulta:
--
--   - NÃO possui GROUP BY;
--   - NÃO possui COUNT/SUM/AVG/etc.;
--   - NÃO agrega vários eventos em uma única linha;
--   - mantém uma linha de saída para cada evento de entrada;
--   - apenas adiciona informações da janela atribuída ao evento.
--
-- Essa query é especialmente útil para estudo e troubleshooting porque
-- permite comparar:
--
--   event_ts
--   window_start
--   window_end
--   window_time
--
-- e verificar se os eventos estão entrando nas janelas esperadas.
--
--
-- Exemplo:
--
--   event_ts = 10:00:12.350
--
-- TUMBLE de 15 segundos:
--
--   window_start = 10:00:00.000
--   window_end   = 10:00:15.000
--   window_time  = 10:00:14.999
--
--
-- Relação temporal:
--
--        event_ts
--           |
--           v
--
--   10:00:00                     10:00:15
--      |-----------------------------|
--      ^                             ^
--      |                             |
-- window_start                  window_end
--
--                               window_time
--                                   |
--                                   v
--                             10:00:14.999
--
--
-- O TUMBLE adiciona três colunas à relação original:
--
--   window_start
--       -> início inclusivo da janela.
--
--   window_end
--       -> limite final da janela.
--          A janela utiliza semanticamente o intervalo:
--
--              [window_start, window_end)
--
--   window_time
--       -> atributo temporal associado à janela.
--
--          Seu valor corresponde a:
--
--              window_end - 1 ms
--
--          Em streaming, `window_time` continua sendo um TIME ATTRIBUTE
--          e pode ser utilizado posteriormente em outras operações
--          temporais, como:
--
--              - outro Window TVF;
--              - cascading windows;
--              - Interval Join;
--              - OVER aggregation;
--              - Window Top-N.
--
--
-- Importante:
--
-- `window_start` e `window_end` descrevem os limites da janela,
-- mas não preservam por si mesmos o atributo ROWTIME para operações
-- temporais subsequentes.
--
-- Para encadear outra operação baseada em tempo, normalmente
-- `window_time` é a coluna relevante.
--
--
-- Esta query responde:
--
--   "Em qual janela de 15 segundos o Flink colocou cada evento?"
--
-- Ela NÃO responde:
--
--   "Quantos eventos existem na janela?"
--
-- Para isso seria necessário adicionar uma agregação, como COUNT(*),
-- e GROUP BY window_start/window_end.
--
-- =============================================================================

SELECT
    id_transacao_pix,
    event_id,
    status_autorizacao,

    -- Event Time original do evento.
    -- É este valor que determina a janela à qual o registro pertence.
    event_ts,

    -- Início da janela TUMBLE atribuída ao evento.
    window_start,

    -- Final da janela TUMBLE atribuída ao evento.
    window_end,

    -- Atributo temporal da janela.
    -- Corresponde a `window_end - 1 ms`.
    -- Pode propagar a semântica temporal para operações posteriores.
    window_time

FROM TABLE(
    TUMBLE(
        TABLE autorizacao,

        -- ROWTIME utilizado para atribuição temporal.
        DESCRIPTOR(event_ts),

        -- Janela fixa e não sobreposta de 15 segundos.
        INTERVAL '15' SECOND
    )
);