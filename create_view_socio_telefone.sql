DROP VIEW IF EXISTS socio_telefone;
CREATE VIEW socio_telefone AS
SELECT DISTINCT
	CONCAT(s.nome_socio,'-',s.cnpj_cpf_socio) AS socio_cnpj_cpf,
	CASE
		WHEN de.telefone1 IS NOT NULL THEN CONCAT(de.ddd1,' ', de.telefone1)
		ELSE CONCAT(de.ddd2,' ', de.telefone2)
	END AS "Telefone"
FROM public.socios AS s
LEFT JOIN public.dados_empresas AS de
ON s.cnpj_basico = de."CNPJ Básico"
WHERE de.telefone1 IS NOT NULL OR de.telefone2 IS NOT NULL;