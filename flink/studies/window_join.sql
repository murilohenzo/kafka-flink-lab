WITH autorizacao_window AS (
    SELECT *
    FROM TABLE(
        TUMBLE(
            TABLE autorizacao,
            DESCRIPTOR(event_ts),
            INTERVAL '15' SECOND
        )
    )
),

antifraude_window AS (
    SELECT *
    FROM TABLE(
        TUMBLE(
            TABLE antifraude,
            DESCRIPTOR(event_ts),
            INTERVAL '15' SECOND
        )
    )
)

SELECT
    a.id_transacao_pix,
    a.event_id AS autorizacao_event_id,
    af.event_id AS antifraude_event_id,

    a.event_ts AS autorizacao_time,
    af.event_ts AS antifraude_time,

    a.window_start,
    a.window_end

FROM autorizacao_window AS a

JOIN antifraude_window AS af

    ON a.id_transacao_pix = af.id_transacao_pix

   AND a.window_start = af.window_start
   AND a.window_end = af.window_end;

-- 

WITH autorizacao_window AS (
    SELECT *
    FROM TABLE(
        TUMBLE(
            TABLE autorizacao,
            DESCRIPTOR(event_ts),
            INTERVAL '15' SECOND
        )
    )
),

antifraude_window AS (
    SELECT *
    FROM TABLE(
        TUMBLE(
            TABLE antifraude,
            DESCRIPTOR(event_ts),
            INTERVAL '15' SECOND
        )
    )
),

liquidacao_window AS (
    SELECT *
    FROM TABLE(
        TUMBLE(
            TABLE liquidacao,
            DESCRIPTOR(event_ts),
            INTERVAL '15' SECOND
        )
    )
)

SELECT
    a.id_transacao_pix,

    a.event_id AS autorizacao_event_id,
    af.event_id AS antifraude_event_id,
    l.event_id AS liquidacao_event_id,

    a.event_ts AS autorizacao_time,
    af.event_ts AS antifraude_time,
    l.event_ts AS liquidacao_time,

    a.window_start,
    a.window_end

FROM autorizacao_window AS a

JOIN antifraude_window AS af
    ON a.id_transacao_pix = af.id_transacao_pix
   AND a.window_start = af.window_start
   AND a.window_end = af.window_end

JOIN liquidacao_window AS l
    ON a.id_transacao_pix = l.id_transacao_pix
   AND a.window_start = l.window_start
   AND a.window_end = l.window_end;