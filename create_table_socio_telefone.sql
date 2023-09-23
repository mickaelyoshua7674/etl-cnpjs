DROP TABLE IF EXISTS public.socio_telefone;
CREATE TABLE public.socio_telefone AS
SELECT
	s.nome_socio,
	s.cnpj_cpf_socio,
    CASE
        WHEN ie.telefone1 IS NOT NULL THEN CONCAT(ie.ddd1,' ',ie.telefone1)
        ELSE CONCAT(ie.ddd2,' ',ie.telefone2)
    END AS telefone
FROM public.socios AS s 

LEFT JOIN public.info_empresa AS ie
ON s.cnpj_basico = ie."CNPJ Básico";