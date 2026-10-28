SELECT
    id_transacao_pix,
    event_id,
    event_ts,
    window_start,
    window_end,
    window_time
FROM TABLE(
    CUMULATE(
        TABLE autorizacao,
        DESCRIPTOR(event_ts),
        INTERVAL '5' SECOND,
        INTERVAL '15' SECOND
    )
);