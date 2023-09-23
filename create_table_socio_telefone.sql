CREATE TABLE socio_telefone AS
SELECT
	s.nome_socio,
	s.cnpj_cpf_socio,
    CASE
        WHEN de.telefone1 IS NOT NULL THEN CONCAT(de.ddd1,' ',de.telefone1)
        ELSE CONCAT(de.ddd2,' ',de.telefone2)
    END AS telefone
FROM public.socios AS s 

LEFT JOIN public.dados_empresas AS de
ON s.cnpj_basico = de."CNPJ Básico";