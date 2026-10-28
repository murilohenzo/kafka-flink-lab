-- Uma saída final por transação observada e janela alinhada de 15 segundos.
-- Presença não significa que os estados de negócio foram aprovados.
SET 'pipeline.name' = 'pix-decisoes-tumble-15s-v2';

CREATE TEMPORARY VIEW pix_presencas_15s AS
SELECT
    id_transacao_pix,
    window_start,
    window_end,
    MAX(CASE WHEN origem = 'AUTH' THEN 1 ELSE 0 END) AS tem_auth,
    MAX(CASE WHEN origem = 'LIQ' THEN 1 ELSE 0 END) AS tem_liq,
    MAX(CASE WHEN origem = 'FRAUD' THEN 1 ELSE 0 END) AS tem_fraud,
    COUNT(*) AS qtd_registros,
    COUNT(DISTINCT event_id) AS qtd_eventos_distintos
FROM TABLE(
    TUMBLE(
        TABLE pix_evidencias,
        DESCRIPTOR(event_ts),
        INTERVAL '15' SECOND
    )
)
GROUP BY id_transacao_pix, window_start, window_end;

CREATE TEMPORARY VIEW pix_decisoes_15s AS
SELECT
    id_transacao_pix,
    window_start,
    window_end,
    tem_auth,
    tem_liq,
    tem_fraud,
    qtd_registros,
    qtd_eventos_distintos,
    CASE
        WHEN tem_auth = 1 AND tem_liq = 1 AND tem_fraud = 1
            THEN 'CONSOLIDATED'
        ELSE 'INCOMPLETE'
    END AS status_correlacao,
    CONCAT(
        CASE WHEN tem_auth = 0 THEN 'AUTH;' ELSE '' END,
        CASE WHEN tem_liq = 0 THEN 'LIQ;' ELSE '' END,
        CASE WHEN tem_fraud = 0 THEN 'FRAUD;' ELSE '' END
    ) AS origens_faltantes,
    'tumble-15s-wm5s-presenca-v2' AS versao_politica
FROM pix_presencas_15s;

-- STATEMENT SET envia os dois destinos pelo mesmo job e plano compartilhado.
EXECUTE STATEMENT SET
BEGIN
    INSERT INTO transacao_consolidada
    SELECT
        id_transacao_pix, window_start, window_end,
        tem_auth, tem_liq, tem_fraud,
        qtd_registros, qtd_eventos_distintos,
        status_correlacao, origens_faltantes, versao_politica,
        CURRENT_TIMESTAMP
    FROM pix_decisoes_15s
    WHERE status_correlacao = 'CONSOLIDATED';

    INSERT INTO transacao_incompleta
    SELECT
        id_transacao_pix, window_start, window_end,
        tem_auth, tem_liq, tem_fraud,
        qtd_registros, qtd_eventos_distintos,
        status_correlacao, origens_faltantes, versao_politica,
        CURRENT_TIMESTAMP
    FROM pix_decisoes_15s
    WHERE status_correlacao = 'INCOMPLETE';
END;
