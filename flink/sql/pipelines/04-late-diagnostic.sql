-- Diagnóstico no ponto de observação deste job. Não representa um side
-- output automático de outro job nem a lista certificada de descartes.
SET 'pipeline.name' = 'pix-late-diagnostic-v2';

INSERT INTO transacao_late_events
SELECT
    id_transacao_pix,
    event_id,
    origem,
    event_time,
    produced_at,
    CURRENT_WATERMARK(event_ts),
    'ATRASADO_NO_PONTO_DE_OBSERVACAO',
    CURRENT_TIMESTAMP
FROM pix_evidencias
WHERE CURRENT_WATERMARK(event_ts) IS NOT NULL
  AND event_ts <= CURRENT_WATERMARK(event_ts);
