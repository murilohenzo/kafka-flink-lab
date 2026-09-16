-- Selecionar eventos de antifraude cujo score_risco seja superior a 70.0.
-- A consulta pode ser usada, por exemplo, para identificar transações que ultrapassam determinado limiar de risco definido pela regra de negócio.

select 
    id_transacao_pix, score_risco
from antifraude
where score_risco > 70.0;