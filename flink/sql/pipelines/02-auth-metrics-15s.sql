-- TUMBLE responde “quanto houve em cada bucket fixo de 15 segundos?”.
-- A contagem e a soma incluem duplicatas presentes no tópico.
SET 'pipeline.name' = 'pix-auth-metrics-15s-v2';

INSERT INTO autorizacao_metricas
SELECT
    window_start,
    window_end,
    COUNT(*) AS quantidade,
    SUM(valor) AS valor_total
FROM TABLE(
    TUMBLE(
        TABLE autorizacao,
        DESCRIPTOR(event_ts),
        INTERVAL '15' SECOND
    )
)
WHERE event_type = 'AUTHORIZATION'
GROUP BY window_start, window_end;
