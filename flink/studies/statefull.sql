-- Quantos eventos de alto risco já observei para este event_id?"

SELECT
    event_id,
    COUNT(score_risco) AS limiar_70
FROM antifraude
WHERE score_risco >= 70.0
GROUP BY event_id;

-- "Este event_id já apareceu mais de uma vez?"

SELECT
    event_id,
    COUNT(*) AS quantidade
FROM antifraude
GROUP BY event_id
HAVING COUNT(*) > 1;