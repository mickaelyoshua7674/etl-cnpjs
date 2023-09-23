CREATE TABLE info_empresa AS
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
	estab.ddd2,
	estab.telefone2,
	estab.ddd_fax AS "DDD FAX",
	estab.fax AS "FAX",
	estab.email AS "Email"
FROM public.estabelecimentos AS estab

LEFT JOIN public.empresas AS emp
USING(cnpj_basico)
LEFT JOIN public.id_natureza_juridica AS nj
USING(natureza_juridica)
LEFT JOIN public.id_qualificacoes AS q
USING(qualificacoes)
LEFT JOIN public.id_porte_empresa AS pe
USING(porte_empresa)

LEFT JOIN public.id_identificador AS ident
USING(identificador)
LEFT JOIN public.id_situacao_cadastral AS sc
USING(situacao_cadastral)
LEFT JOIN public.id_motivo_situacao_cadastral AS msc
USING(motivo_situacao_cadastral)
LEFT JOIN public.paises AS paises
USING(cod_pais)
LEFT JOIN public.municipios AS mun
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
	((estab.ddd1 IS NOT NULL AND estab.telefone1 IS NOT NULL) OR
	 (estab.ddd2 IS NOT NULL AND estab.telefone2 IS NOT NULL)) AND
	sc.situacao_cadastral = 2; -- ATIVA