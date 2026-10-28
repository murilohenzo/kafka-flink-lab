-- Cada registro aprovado gera uma saída append-only.
-- A query preserva o instante do evento e da publicação e acrescenta
-- o instante de processamento do Flink.
SET 'pipeline.name' = 'pix-auth-approved-v2';

INSERT INTO autorizacao_aprovada
SELECT
    id_transacao_pix,
    event_id,
    event_type,
    event_version,
    valor,
    status_autorizacao,
    event_time,
    produced_at,
    CURRENT_TIMESTAMP
FROM autorizacao
WHERE event_type = 'AUTHORIZATION'
  AND status_autorizacao = 'APROVADA';
