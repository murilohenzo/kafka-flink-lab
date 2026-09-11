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

    processed_at TIMESTAMP_LTZ(3)

)
WITH (

    'connector' = 'kafka',

    'topic' = 'pix.autorizacao.aprovada',

    'properties.bootstrap.servers' = 'kafka:29092',

    'format' = 'json'

);


-- =============================================================
-- SINK
-- MÉTRICAS DE AUTORIZAÇÃO
-- =============================================================

CREATE TABLE autorizacao_metricas (

    window_start TIMESTAMP(3),

    window_end TIMESTAMP(3),

    quantidade BIGINT,

    valor_total DECIMAL(20, 2)

)
WITH (

    'connector' = 'kafka',

    'topic' = 'pix.autorizacao.metricas',

    'properties.bootstrap.servers' = 'kafka:29092',

    'format' = 'json'

);


-- =============================================================
-- SINK
-- PIX CONSOLIDADO
-- =============================================================

CREATE TABLE transacao_consolidada (

    id_transacao_pix STRING,

    valor DECIMAL(12, 2),

    status_autorizacao STRING,

    status_liquidacao STRING,

    status_antifraude STRING,

    score_risco DOUBLE,

    authorization_event_id STRING,

    settlement_event_id STRING,

    fraud_event_id STRING,

    consolidated_at TIMESTAMP_LTZ(3)

)
WITH (

    'connector' = 'kafka',

    'topic' = 'pix.transacao.consolidada',

    'properties.bootstrap.servers' = 'kafka:29092',

    'format' = 'json'

);


-- =============================================================
-- SINK
-- PIX INCOMPLETO
-- =============================================================

CREATE TABLE transacao_incompleta (

    id_transacao_pix STRING,

    missing_sources STRING,

    window_start TIMESTAMP_LTZ(3),

    window_end TIMESTAMP_LTZ(3),

    reason STRING,

    detected_at TIMESTAMP_LTZ(3)

)
WITH (

    'connector' = 'kafka',

    'topic' = 'pix.transacao.incompleta',

    'properties.bootstrap.servers' = 'kafka:29092',

    'format' = 'json'

);


-- =============================================================
-- SINK
-- PIX LATE EVENTS
-- =============================================================

CREATE TABLE transacao_late_events (

    id_transacao_pix STRING,

    event_id STRING,

    event_type STRING,

    event_time BIGINT,

    detected_watermark TIMESTAMP_LTZ(3),

    detected_at TIMESTAMP_LTZ(3)

)
WITH (

    'connector' = 'kafka',

    'topic' = 'pix.transacao.late-events',

    'properties.bootstrap.servers' = 'kafka:29092',

    'format' = 'json'

);