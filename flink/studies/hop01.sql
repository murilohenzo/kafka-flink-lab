-- =============================================================================
-- HOP WINDOW — CONCEITO BASE
-- =============================================================================
--
-- HOP cria janelas de tamanho fixo que avançam em intervalos definidos
-- pelo parâmetro SLIDE.
--
-- Nesta configuração:
--
--   SLIDE = 5 segundos
--   SIZE  = 15 segundos
--
-- Uma nova janela começa a cada 5 segundos e cada janela possui
-- duração total de 15 segundos.
--
-- Exemplo:
--
--   Window 1: [10:00:00, 10:00:15)
--   Window 2: [10:00:05, 10:00:20)
--   Window 3: [10:00:10, 10:00:25)
--   Window 4: [10:00:15, 10:00:30)
--
-- Como:
--
--   SLIDE < SIZE
--
-- as janelas se SOBREPÕEM.
--
-- Dessa forma, um único evento pode participar de várias janelas.
--
-- Exemplo:
--
--   event_ts = 10:00:12
--
-- pertence simultaneamente a:
--
--   [10:00:00, 10:00:15)
--   [10:00:05, 10:00:20)
--   [10:00:10, 10:00:25)
--
-- Com:
--
--   SIZE / SLIDE = 15 / 5 = 3
--
-- um evento normalmente participa de 3 janelas HOP.
--
-- =============================================================================



-- =============================================================================
-- HOP #1 — INSPECIONAR A QUAIS JANELAS CADA EVENTO PERTENCE
-- =============================================================================
--
-- Objetivo:
--
--   Visualizar os eventos individualmente e verificar a quais janelas
--   HOP cada evento foi atribuído.
--
-- Não existe GROUP BY.
-- Não existe agregação.
--
-- O HOP apenas transforma:
--
--   evento
--
-- em:
--
--   evento + window_start + window_end + window_time
--
--
-- Esta é provavelmente a melhor query para estudar a diferença
-- entre TUMBLE e HOP.
--
-- TUMBLE:
--
--   evento e1
--       |
--       +------> uma única janela
--
-- HOP:
--
--   evento e1
--       |
--       +------> Window A
--       +------> Window B
--       +------> Window C
--
--
-- Pergunta respondida:
--
--   "A quais janelas de 15 segundos este evento pertence,
--    considerando que uma nova janela começa a cada 5 segundos?"
--
-- =============================================================================

SELECT
    id_transacao_pix,
    event_id,
    status_autorizacao,
    valor,
    event_ts,

    -- Início de uma das janelas às quais o evento foi atribuído.
    window_start,

    -- Limite final da janela.
    window_end,

    -- Time attribute da janela.
    -- Corresponde semanticamente a window_end - 1 ms.
    window_time

FROM TABLE(
    HOP(
        TABLE autorizacao,

        -- event_ts utilizado para determinar a posição temporal
        -- do evento.
        DESCRIPTOR(event_ts),

        -- SLIDE:
        -- inicia uma nova janela a cada 5 segundos.
        INTERVAL '5' SECOND,

        -- SIZE:
        -- cada janela possui duração total de 15 segundos.
        INTERVAL '15' SECOND
    )
);