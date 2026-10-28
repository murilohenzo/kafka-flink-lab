-- =============================================================================
-- TUMBLE #1 — AGREGAÇÃO DE EVENTOS POR JANELA DE EVENT TIME
-- =============================================================================
--
-- Objetivo:
--   Contar quantos eventos de cada `event_type`, com `valor >= 50.00`,
--   ocorreram dentro de cada janela fixa de 15 segundos de EVENT TIME.
--
-- Conceitos exercitados:
--   - Event Time
--   - event_ts (`event_ts`)
--   - Watermark da tabela de origem
--   - TUMBLE Window TVF
--   - Window Aggregation
--   - GROUP BY temporal
--   - processamento STATEFUL
--
-- Premissa da tabela `autorizacao`:
--
--   event_ts AS TO_TIMESTAMP_LTZ(event_time, 3)
--
--   WATERMARK FOR event_ts AS
--       event_ts - INTERVAL '5' SECOND
--
-- Portanto:
--
--   `event_ts`
--       -> determina EM QUAL janela o evento será colocado.
--
--   WATERMARK
--       -> representa o progresso do event time e permite ao operador
--          determinar quando uma janela pode ser concluída.
--
--   INTERVAL '15' SECOND
--       -> determina o TAMANHO da janela.
--
-- Importante:
--
--   WATERMARK de 5 segundos NÃO significa:
--
--       janela de 15s + watermark de 5s = janela de 20s
--
--   A janela continua tendo exatamente 15 segundos.
--
--
-- Exemplo de distribuição:
--
--   event_ts = 10:00:02 ─┐
--   event_ts = 10:00:07  ├──> [10:00:00, 10:00:15)
--   event_ts = 10:00:13 ─┘
--
--   event_ts = 10:00:17 ─┐
--   event_ts = 10:00:22 ─┴──> [10:00:15, 10:00:30)
--
--
-- O operador é STATEFUL porque precisa acumular os eventos pertencentes
-- à mesma combinação:
--
--   (event_type, window_start, window_end)
--
-- Exemplo conceitual de state:
--
--   (AUTHORIZATION, 10:00:00, 10:00:15) -> COUNT = 3
--   (CANCEL,        10:00:00, 10:00:15) -> COUNT = 1
--
--
-- O WHERE elimina eventos com `valor < 50.00` da agregação.
--
-- O GROUP BY contém:
--
--   event_type
--   window_start
--   window_end
--
-- `window_start` e `window_end` são fundamentais para que o Flink
-- reconheça esta operação como uma WINDOW AGGREGATION.
--
-- Diferentemente de um GROUP BY contínuo, a Window Aggregation possui
-- um horizonte temporal. Quando a janela é concluída, o Flink produz
-- o resultado agregado da janela e pode descartar o state intermediário
-- que não é mais necessário.
--
-- Pergunta respondida:
--
--   "Quantos eventos de cada tipo, com valor >= 50, ocorreram
--    em cada intervalo de 15 segundos de event time?"
--
-- =============================================================================

SELECT
    event_type,
    window_start,
    window_end,
    COUNT(*) AS quantidade
FROM TABLE(
    TUMBLE(
        TABLE autorizacao,

        -- Coluna event_ts utilizada para atribuir cada evento
        -- à sua respectiva janela de EVENT TIME.
        DESCRIPTOR(event_ts),

        -- Cria janelas fixas, consecutivas e não sobrepostas
        -- com duração de 15 segundos.
        INTERVAL '15' SECOND
    )
)

-- Filtra os registros que participarão da agregação.
WHERE valor >= 50.00

-- Define a granularidade do state e do resultado agregado:
-- uma linha para cada event_type dentro de cada janela.
GROUP BY
    event_type,
    window_start,
    window_end;