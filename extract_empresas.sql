SELECT
	"CNPJ",
	"CNPJ Básico",
	"CNPJ Ordem",
	"CNPJ Digito Verificador",
	"Identificador",
	"Razão Social",
	"Nome Fantasia",
	"Natureza Jurídica",
	"Opção Simples",
	"Opção MEI",
	"Qualificação do Responsável",
	"Capital Social",
	"Porte Empresa",
	"Ente Federativo Responsável",
	"Data Situação Cadastral",
	"Motivo Situação Cadastral",
	"Quantidade Sócios",
	"Nome Cidade Exterior",
	"País",
	"Data Início Atividade",
	"CNAE Fiscal Principal",
	"Tipo Logradouro",
	"Logradouro",
	"Número",
	"Complemento",
	"Bairro",
	"CEP",
	"Unidade Federativa",
	"Município",
	"DDD FAX",
	"FAX",
	"Email"
FROM public.info_empresa

WHERE
	"CNAE Fiscal Principal" = 9602501 AND
	"Município" = 'MARINGA'

ORDER BY RANDOM()
LIMIT 50;