DROP TABLE IF EXISTS public.empresas;
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_identificador (
    identificador INTEGER NOT NULL,
    descricao CHARACTER (6) NOT NULL,
    PRIMARY KEY (identificador)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_situacao_cadastral (
    situacao_cadastral CHARACTER VARYING(2) NOT NULL,
    descricao CHARACTER VARYING(8) NOT NULL,
    PRIMARY KEY (situacao_cadastral)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_opcao_simples (
    opcao_simples CHARACTER(1) NOT NULL,
    descricao CHARACTER VARYING(6) NOT NULL,
    PRIMARY KEY (opcao_simples)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_opcao_mei (
    opcao_mei CHARACTER(1) NOT NULL,
    descricao CHARACTER VARYING(6) NOT NULL,
    PRIMARY KEY (opcao_mei)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.empresas (
    cnpj_basico CHARACTER(8) NOT NULL,
    razao_social CHARACTER VARYING(300),
    natureza_juridica CHARACTER VARYING(300),
    qualificacoes_responsavel CHARACTER VARYING(300),
    capital_social NUMERIC,
    porte_empresa CHARACTER(2),
    ente_federativo_responsavel CHARACTER VARYING(300)
    PRIMARY KEY (cnpj_basico)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.estabelecimentos (
    cnpj_basico CHARACTER(8) NOT NULL,
    cnpj_ordem CHARACTER(4) NOT NULL,
    cnpj_dv CHARACTER(2) NOT NULL,
    identificador INTEGER NOT NULL,
    nome_fantasia CHARACTER VARYING(300),
    situacao_cadastral CHARACTER VARYING(2) NOT NULL,
    data_situacao_cadastral CHARACTER VARYING(300),
    motivo_situacao_cadastral CHARACTER VARYING(300),
    nome_cidade_exterior CHARACTER VARYING(300),
    pais CHARACTER VARYING(300),
    data_inicio_atividade CHARACTER VARYING(300),
    cnae_fiscal_principal CHARACTER VARYING(300),
    cnae_fiscal_secundario CHARACTER VARYING(300),
    tipo_logradouro CHARACTER VARYING(300),
    logradouro CHARACTER VARYING(300),
    numero CHARACTER VARYING(300),
    complemento CHARACTER VARYING(300),
    bairro CHARACTER VARYING(300),
    cep CHARACTER VARYING(300),
    uf CHARACTER VARYING(300),
    municipio CHARACTER VARYING(300),
    ddd1 CHARACTER VARYING(300),
    telefone1 CHARACTER VARYING(300),
    ddd2 CHARACTER VARYING(300),
    telefone2 CHARACTER VARYING(300),
    ddd_fax CHARACTER VARYING(300),
    fax CHARACTER VARYING(300),
    email CHARACTER VARYING(300),
    situacao_especial CHARACTER VARYING(300),
    data_situacao_especial CHARACTER VARYING(300),
    CONSTRAINT identificador FOREIGN KEY (identificador) REFERENCES public.id_identificador(identificador),
    CONSTRAINT situacao_cadastral FOREIGN KEY (situacao_cadastral) REFERENCES public.id_situacao_cadastral(situacao_cadastral)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.simples (
    cnpj_basico CHARACTER(8) NOT NULL,
    opcao_simples CHARACTER(1) NOT NULL,
    data_opcao_simples CHARACTER VARYING(300),
    data_exclusao_simples CHARACTER VARYING(300),
    opcao_mei CHARACTER VARYING(300) NOT NULL,
    data_opcao_mei CHARACTER VARYING(300),
    data_exclusao_mei CHARACTER VARYING(300),
    CONSTRAINT opcao_simples FOREIGN KEY (opcao_simples) REFERENCES public.id_opcao_simples(opcao_simples),
    CONSTRAINT opcao_mei FOREIGN KEY (opcao_mei) REFERENCES public.id_opcao_mei(opcao_mei)
);
----------------------------------------------------------------------------------------------------