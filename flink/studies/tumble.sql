SELECT *
FROM TABLE(
    TUMBLE(
        TABLE autorizacao,
        DESCRIPTOR(rowtime),
        INTERVAL '15' SECOND
    )
);

-- 

SELECT
    id_transacao_pix,
    event_id,
    status_autorizacao,
    rowtime,
    window_start,
    window_end,
    window_time
FROM TABLE(
    TUMBLE(
        TABLE autorizacao,
        DESCRIPTOR(rowtime),
        INTERVAL '15' SECOND
    )
);