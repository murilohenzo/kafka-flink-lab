SELECT
    window_start,
    window_end,
    COUNT(*) AS total_autorizacoes,
    SUM(valor) AS valor_total
FROM TABLE(
    TUMBLE(
        TABLE autorizacao,
        DESCRIPTOR(rowtime),
        INTERVAL '15' SECOND
    )
)
GROUP BY
    window_start,
    window_end;

-- 

SELECT
    id_transacao_pix,
    window_start,
    window_end,
    COUNT(*) AS total_eventos
FROM TABLE(
    TUMBLE(
        TABLE autorizacao,
        DESCRIPTOR(rowtime),
        INTERVAL '15' SECOND
    )
)
GROUP BY
    id_transacao_pix,
    window_start,
    window_end;