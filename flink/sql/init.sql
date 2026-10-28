-- =============================================================
-- Kafka + Flink PIX Lab
--
-- Inicialização padrão do Flink SQL Client.
--
-- Executado automaticamente através de:
--
--   sql-client.sh -i /opt/flink/sql/init.sql
--
-- IMPORTANTE:
--
-- O catálogo default do SQL Client é de sessão.
-- Este arquivo recria automaticamente as tabelas sempre que
-- uma nova sessão é iniciada.
-- =============================================================


-- =============================================================
-- CONFIGURAÇÕES DA SESSÃO
-- =============================================================

SET 'execution.runtime-mode' = 'streaming';

SET 'parallelism.default' = '3';

SET 'sql-client.execution.result-mode' = 'tableau';

SET 'sql-client.verbose' = 'true';


-- =============================================================
-- SOURCE
-- PIX AUTORIZAÇÃO
-- =============================================================

CREATE TABLE autorizacao (

    id_transacao_pix STRING,

    event_id STRING,

    event_type STRING,

    event_version INT,

    valor DECIMAL(12, 2),

    status_autorizacao STRING,

    event_time BIGINT,

    produced_at BIGINT,

    -- ---------------------------------------------------------
    -- Event Time
    -- ---------------------------------------------------------

    event_ts AS TO_TIMESTAMP_LTZ(
        event_time,
        3
    ),

    -- ---------------------------------------------------------
    -- Watermark
    --
    -- Aceita aproximadamente 5 segundos de eventos
    -- fora de ordem.
    -- ---------------------------------------------------------

    WATERMARK FOR event_ts
        AS event_ts - INTERVAL '5' SECOND

)
WITH (

    'connector' = 'kafka',

    'topic' = 'pix.autorizacao.eventos',

    'properties.bootstrap.servers' = 'kafka:29092',

    'properties.group.id' = 'flink-pix-autorizacao',

    'scan.startup.mode' = 'earliest-offset',

    'format' = 'json',

    'json.ignore-parse-errors' = 'false'

);


-- =============================================================
-- SOURCE
-- PIX LIQUIDAÇÃO
-- =============================================================

CREATE TABLE liquidacao (

    id_transacao_pix STRING,

    event_id STRING,

    event_type STRING,

    event_version INT,

    id_liquidacao_spi STRING,

    status_liquidacao STRING,

    event_time BIGINT,

    produced_at BIGINT,

    event_ts AS TO_TIMESTAMP_LTZ(
        event_time,
        3
    ),

    WATERMARK FOR event_ts
        AS event_ts - INTERVAL '5' SECOND

)
WITH (

    'connector' = 'kafka',

    'topic' = 'pix.liquidacao.eventos',

    'properties.bootstrap.servers' = 'kafka:29092',

    'properties.group.id' = 'flink-pix-liquidacao',

    'scan.startup.mode' = 'earliest-offset',

    'format' = 'json',

    'json.ignore-parse-errors' = 'false'

);


-- =============================================================
-- SOURCE
-- PIX ANTIFRAUDE
-- =============================================================

CREATE TABLE antifraude (

    id_transacao_pix STRING,

    event_id STRING,

    event_type STRING,

    event_version INT,

    score_risco DOUBLE,

    status_antifraude STRING,

    event_time BIGINT,

    produced_at BIGINT,

    event_ts AS TO_TIMESTAMP_LTZ(
        event_time,
        3
    ),

    WATERMARK FOR event_ts
        AS event_ts - INTERVAL '5' SECOND

)
WITH (

    'connector' = 'kafka',

    'topic' = 'pix.antifraude.eventos',

    'properties.bootstrap.servers' = 'kafka:29092',

    'properties.group.id' = 'flink-pix-antifraude',

    'scan.startup.mode' = 'earliest-offset',

    'format' = 'json',

    'json.ignore-parse-errors' = 'false'

);


-- =============================================================
-- SINK
-- AUTORIZAÇÕES APROVADAS
-- =============================================================

CREATE TABLE autorizacao_aprovada (

    id_transacao_pix STRING,

    event_id STRING,

    event_type STRING,

    event_version INT,

    valor DECIMAL(12, 2),

    status_autorizacao STRING,

    event_time BIGINT,

    produced_at BIGINT,

    processed_at TIMESTAMP_LTZ(3)

)
WITH (

    'connector' = 'kafka',

    'topic' = 'pix.autorizacao.aprovada',

    'properties.bootstrap.servers' = 'kafka:29092',

    'format' = 'json',

    'sink.delivery-guarantee' = 'at-least-once'

);


-- =============================================================
-- SINK
-- MÉTRICAS DE AUTORIZAÇÃO
-- =============================================================

CREATE TABLE autorizacao_metricas (

    window_start TIMESTAMP_LTZ(3),

    window_end TIMESTAMP_LTZ(3),

    quantidade BIGINT,

    valor_total DECIMAL(38, 2)

)
WITH (

    'connector' = 'kafka',

    'topic' = 'pix.autorizacao.metricas',

    'properties.bootstrap.servers' = 'kafka:29092',

    'format' = 'json',

    'sink.delivery-guarantee' = 'at-least-once'

);


-- =============================================================
-- SINK
-- PIX CONSOLIDADO
-- =============================================================

CREATE TABLE transacao_consolidada (

    id_transacao_pix STRING,

    window_start TIMESTAMP_LTZ(3),

    window_end TIMESTAMP_LTZ(3),

    tem_auth INT,

    tem_liq INT,

    tem_fraud INT,

    qtd_registros BIGINT,

    qtd_eventos_distintos BIGINT,

    status_correlacao STRING,

    origens_faltantes STRING,

    versao_politica STRING,

    emitido_em TIMESTAMP_LTZ(3)

)
WITH (

    'connector' = 'kafka',

    'topic' = 'pix.transacao.consolidada.presenca.v2',

    'properties.bootstrap.servers' = 'kafka:29092',

    'key.format' = 'raw',

    'key.fields' = 'id_transacao_pix',

    'value.format' = 'json',

    'value.fields-include' = 'ALL',

    'sink.delivery-guarantee' = 'at-least-once'

);


-- =============================================================
-- SINK
-- PIX INCOMPLETO
-- =============================================================

CREATE TABLE transacao_incompleta (

    id_transacao_pix STRING,

    window_start TIMESTAMP_LTZ(3),

    window_end TIMESTAMP_LTZ(3),

    tem_auth INT,

    tem_liq INT,

    tem_fraud INT,

    qtd_registros BIGINT,

    qtd_eventos_distintos BIGINT,

    status_correlacao STRING,

    origens_faltantes STRING,

    versao_politica STRING,

    emitido_em TIMESTAMP_LTZ(3)

)
WITH (

    'connector' = 'kafka',

    'topic' = 'pix.transacao.incompleta.presenca.v2',

    'properties.bootstrap.servers' = 'kafka:29092',

    'key.format' = 'raw',

    'key.fields' = 'id_transacao_pix',

    'value.format' = 'json',

    'value.fields-include' = 'ALL',

    'sink.delivery-guarantee' = 'at-least-once'

);


-- =============================================================
-- SINK
-- PIX LATE EVENTS
-- =============================================================

CREATE TABLE transacao_late_events (

    id_transacao_pix STRING,

    event_id STRING,

    origem STRING,

    event_time BIGINT,

    produced_at BIGINT,

    watermark_observado TIMESTAMP_LTZ(3),

    classificacao STRING,

    observado_em TIMESTAMP_LTZ(3)

)
WITH (

    'connector' = 'kafka',

    'topic' = 'pix.transacao.late-events',

    'properties.bootstrap.servers' = 'kafka:29092',

    'format' = 'json',

    'sink.delivery-guarantee' = 'at-least-once'

);


-- ============================================================
-- VIEW NORMALIZADA
-- Uma linha por evidência, preservando a propriedade event_ts.
-- ============================================================

CREATE TEMPORARY VIEW pix_evidencias AS
SELECT
    id_transacao_pix,
    event_id,
    event_version,
    'AUTH' AS origem,
    status_autorizacao AS status_evento,
    event_time,
    produced_at,
    event_ts
FROM autorizacao
WHERE id_transacao_pix IS NOT NULL
  AND event_id IS NOT NULL
  AND event_time IS NOT NULL
  AND event_type = 'AUTHORIZATION'

UNION ALL

SELECT
    id_transacao_pix,
    event_id,
    event_version,
    'LIQ' AS origem,
    status_liquidacao AS status_evento,
    event_time,
    produced_at,
    event_ts
FROM liquidacao
WHERE id_transacao_pix IS NOT NULL
  AND event_id IS NOT NULL
  AND event_time IS NOT NULL
  AND event_type = 'SETTLEMENT'

UNION ALL

SELECT
    id_transacao_pix,
    event_id,
    event_version,
    'FRAUD' AS origem,
    status_antifraude AS status_evento,
    event_time,
    produced_at,
    event_ts
FROM antifraude
WHERE id_transacao_pix IS NOT NULL
  AND event_id IS NOT NULL
  AND event_time IS NOT NULL
  AND event_type = 'FRAUD';
