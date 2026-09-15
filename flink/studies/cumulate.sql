SELECT
    id_transacao_pix,
    event_id,
    rowtime,
    window_start,
    window_end,
    window_time
FROM TABLE(
    CUMULATE(
        TABLE autorizacao,
        DESCRIPTOR(rowtime),
        INTERVAL '5' SECOND,
        INTERVAL '15' SECOND
    )
);