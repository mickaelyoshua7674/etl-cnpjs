SELECT
	CONCAT(LEFT(estab.cnpj_basico,2), '.',
		   SUBSTRING(estab.cnpj_basico FROM 3 FOR 3), '.',
		   RIGHT(estab.cnpj_basico,3), '/',
		   estab.cnpj_ordem, '-',
		   estab.cnpj_dv) AS "CNPJ",
	estab.cnpj_basico AS "CNPJ Básico",
	estab.cnpj_ordem AS "CNPJ Ordem",
	estab.cnpj_dv AS "CNPJ Digito Verificador",
	ident.descricao AS "Identificador",
	emp.razao_social AS "Razão Social",
	estab.nome_fantasia AS "Nome Fantasia",
	nj.descricao AS "Natureza Jurídica",
	os.descricao AS "Opção Simples",
	om.descricao AS "Opção MEI",
	q.descricao AS "Qualificação do Responsável",
	emp.capital_social AS "Capital Social",
	pe.descricao AS "Porte Empresa",
	emp.ente_federativo_responsavel AS "Ente Federativo Responsável",
	sc.descricao AS "Situação Cadastral",
	CONCAT(RIGHT(estab.data_situacao_cadastral,2), '/',
		   SUBSTRING(estab.data_situacao_cadastral FROM 5 FOR 2), '/',
		   LEFT(estab.data_situacao_cadastral,4)) AS "Data Situação Cadastral",
	msc.descricao AS "Motivo Situação Cadastral",
	count_socios.count_s AS "Quantidade Sócios",
	estab.nome_cidade_exterior AS "Nome Cidade Exterior",
	paises.pais AS "País",
	CONCAT(RIGHT(estab.data_inicio_atividade,2), '/',
		   SUBSTRING(estab.data_inicio_atividade FROM 5 FOR 2), '/',
		   LEFT(estab.data_inicio_atividade,4)) AS "Data Início Atividade",
	estab.cnae_fiscal_principal AS "CNAE Fiscal Principal",
	estab.tipo_logradouro AS "Tipo Logradouro",
	estab.logradouro AS "Logradouro",
	estab.numero AS "Número",
	estab.complemento AS "Complemento",
	estab.bairro AS "Bairro",
	estab.cep AS "CEP",
	estab.uf AS "Unidade Federativa",
	mun.descricao AS "Município",
	estab.ddd1,
	estab.telefone1,
	CASE
		WHEN estab.ddd1 IS NOT NULL THEN 
			CONCAT('(', estab.ddd1, ') ',
				  LEFT(estab.telefone1,4), '-',
				  RIGHT(estab.telefone1,4))
		ELSE
			estab.ddd1
	END AS "Telefone 1",
	estab.ddd2,
	estab.telefone2,
	CASE
		WHEN estab.ddd2 IS NOT NULL THEN 
			CONCAT('(', estab.ddd2, ') ',
				  LEFT(estab.telefone2,4), '-',
				  RIGHT(estab.telefone2,4))
		ELSE
			estab.ddd2
	END AS "Telefone 2",
	estab.ddd_fax AS "DDD FAX",
	estab.fax AS "FAX",
	estab.email AS "Email"
FROM public.estabelecimentos AS estab

JOIN public.empresas AS emp
USING(cnpj_basico)
JOIN public.id_natureza_juridica AS nj
USING(natureza_juridica)
JOIN public.id_qualificacoes AS q
USING(qualificacoes)
JOIN public.id_porte_empresa AS pe
USING(porte_empresa)

JOIN public.id_identificador AS ident
USING(identificador)
JOIN public.id_situacao_cadastral AS sc
USING(situacao_cadastral)
JOIN public.id_motivo_situacao_cadastral AS msc
USING(motivo_situacao_cadastral)
JOIN public.paises AS paises
USING(cod_pais)
JOIN public.municipios AS mun
USING(cod_municipio)

LEFT JOIN public.simples AS s
USING(cnpj_basico)
LEFT JOIN public.id_opcao_simples AS os
USING(opcao_simples)
LEFT JOIN public.id_opcao_mei AS om
USING(opcao_mei)

JOIN (
	SELECT
		cnpj_basico,
		COUNT(cnpj_cpf_socio) AS count_s
	FROM public.socios
	GROUP BY cnpj_basico
) AS count_socios
USING(cnpj_basico)

WHERE
	estab.cnae_fiscal_principal = 4646001 AND
	estab.uf IN ('MG', 'PR', 'GO', 'DF', 'MT', 'MS') AND
	estab.logradouro IS NOT NULL AND
	estab.telefone1 IS NOT NULL AND
	estab.email IS NOT NULL
	
ORDER BY RANDOM()
LIMIT 1000;