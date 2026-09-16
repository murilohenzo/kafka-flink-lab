-- =============================================================================
-- HOP #5 — MÉTRICAS FINANCEIRAS POR JANELA
-- =============================================================================
--
-- Objetivo:
--
--   Calcular métricas sobre o valor das autorizações
--   dentro de cada janela móvel.
--
-- Métricas:
--
--   COUNT(*)    -> quantidade de eventos
--   SUM(valor)  -> valor total
--   AVG(valor)  -> valor médio
--   MIN(valor)  -> menor valor
--   MAX(valor)  -> maior valor
--
--
-- Esta query permite visualizar tendências móveis.
--
-- Exemplo:
--
-- Janela:
--
--   [10:00:00,10:00:15)
--
-- Valor total:
--
--   15.000
--
-- Próxima janela:
--
--   [10:00:05,10:00:20)
--
-- Valor total:
--
--   17.500
--
-- A segunda janela reutiliza parte do intervalo temporal da primeira.
--
-- Pergunta respondida:
--
--   "Como volume e valores financeiros evoluem utilizando
--    uma janela móvel de 15 segundos?"
--
-- =============================================================================

SELECT
    window_start,
    window_end,

    COUNT(*) AS quantidade,

    SUM(valor) AS valor_total,

    AVG(valor) AS valor_medio,

    MIN(valor) AS menor_valor,

    MAX(valor) AS maior_valor

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