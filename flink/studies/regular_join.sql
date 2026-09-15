-- REGULAR INNER JOIN
-- Não existe limite temporal na condição de JOIN.
-- Em streaming contínuo, o state pode crescer continuamente.

SELECT
    a.id_transacao_pix,
    a.event_id AS autorizacao_event_id,
    af.event_id AS antifraude_event_id,
    l.event_id AS liquidacao_event_id,
    a.rowtime AS autorizacao_time,
    af.rowtime AS antifraude_time,
    l.rowtime AS liquidacao_time
FROM autorizacao AS a
JOIN antifraude AS af
    ON a.id_transacao_pix = af.id_transacao_pix
JOIN liquidacao AS l
    ON a.id_transacao_pix = l.id_transacao_pix;