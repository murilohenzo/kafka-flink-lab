-- INTERVAL JOIN
-- Correlação limitada por event-time.

SELECT
    a.id_transacao_pix,

    a.event_id AS autorizacao_event_id,
    af.event_id AS antifraude_event_id,
    l.event_id AS liquidacao_event_id,

    a.event_ts AS autorizacao_time,
    af.event_ts AS antifraude_time,
    l.event_ts AS liquidacao_time

FROM autorizacao AS a

JOIN antifraude AS af
    ON a.id_transacao_pix = af.id_transacao_pix
   AND af.event_ts BETWEEN
       a.event_ts - INTERVAL '5' SECOND
       AND
       a.event_ts + INTERVAL '15' SECOND

JOIN liquidacao AS l
    ON a.id_transacao_pix = l.id_transacao_pix
   AND l.event_ts BETWEEN
       a.event_ts - INTERVAL '5' SECOND
       AND
       a.event_ts + INTERVAL '15' SECOND;