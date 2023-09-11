-- CREATE DATABASE public_cnpj
--     WITH
--     OWNER = postgres
--     ENCODING = 'UTF8'
--     CONNECTION LIMIT = -1
--     IS_TEMPLATE = False;

DROP TABLE IF EXISTS public.empresas CASCADE;
DROP TABLE IF EXISTS public.estabelecimentos CASCADE;
DROP TABLE IF EXISTS public.simples CASCADE;
DROP TABLE IF EXISTS public.socios CASCADE;
DROP TABLE IF EXISTS public.id_porte_empresa;
DROP TABLE IF EXISTS public.id_identificador;
DROP TABLE IF EXISTS public.id_situacao_cadastral;
DROP TABLE IF EXISTS public.id_opcao_simples;
DROP TABLE IF EXISTS public.id_opcao_mei;
DROP TABLE IF EXISTS public.id_identificador_socio;
DROP TABLE IF EXISTS public.paises;
DROP TABLE IF EXISTS public.municipios;
DROP TABLE IF EXISTS public.id_qualificacoes;
DROP TABLE IF EXISTS public.id_natureza_juridica;
DROP TABLE IF EXISTS public.id_cnae_fiscal_principal;
DROP TABLE IF EXISTS public.id_motivo_situacao_cadastral;
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_porte_empresa (
    porte_empresa INTEGER NOT NULL,
    descricao CHARACTER VARYING(30) NOT NULL,
    PRIMARY KEY (porte_empresa)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_identificador (
    identificador INTEGER NOT NULL,
    descricao CHARACTER VARYING(6) NOT NULL,
    PRIMARY KEY (identificador)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_situacao_cadastral (
    situacao_cadastral INTEGER NOT NULL,
    descricao CHARACTER VARYING(8) NOT NULL,
    PRIMARY KEY (situacao_cadastral)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_opcao_simples (
    opcao_simples INTEGER NOT NULL,
    descricao CHARACTER VARYING(6) NOT NULL,
    PRIMARY KEY (opcao_simples)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_opcao_mei (
    opcao_mei INTEGER NOT NULL,
    descricao CHARACTER VARYING(6) NOT NULL,
    PRIMARY KEY (opcao_mei)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_identificador_socio (
    identificador_socio INTEGER NOT NULL,
    descricao CHARACTER VARYING(15) NOT NULL,
    PRIMARY KEY (identificador_socio)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.paises (
    cod_pais INTEGER NOT NULL,
    pais CHARACTER VARYING(200) NOT NULL,
    PRIMARY KEY (cod_pais)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.municipios (
    cod_municipio INTEGER NOT NULL,
    descricao CHARACTER VARYING(200) NOT NULL,
    PRIMARY KEY (cod_municipio)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_qualificacoes (
    qualificacoes INTEGER NOT NULL,
    descricao CHARACTER VARYING(200) NOT NULL,
    PRIMARY KEY (qualificacoes)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_natureza_juridica (
    natureza_juridica INTEGER NOT NULL,
    descricao CHARACTER VARYING(200) NOT NULL,
    PRIMARY KEY (natureza_juridica)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_cnae_fiscal_principal (
    cnae_fiscal_principal INTEGER NOT NULL,
    descricao CHARACTER VARYING(200) NOT NULL,
    PRIMARY KEY (cnae_fiscal_principal)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_motivo_situacao_cadastral (
    motivo_situacao_cadastral INTEGER NOT NULL,
    descricao CHARACTER VARYING(200),
    PRIMARY KEY (motivo_situacao_cadastral)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.empresas (
    cnpj_basico CHARACTER(8) NOT NULL,
    razao_social CHARACTER VARYING(200),
    natureza_juridica INTEGER NOT NULL,
    qualificacoes INTEGER NOT NULL,
    capital_social NUMERIC,
    porte_empresa INTEGER NOT NULL,
    ente_federativo_responsavel CHARACTER VARYING(200),
    CONSTRAINT natureza_juridica FOREIGN KEY (natureza_juridica) REFERENCES public.id_natureza_juridica(natureza_juridica),
    CONSTRAINT porte_empresa FOREIGN KEY (porte_empresa) REFERENCES public.id_porte_empresa(porte_empresa),
    CONSTRAINT qualificacoes FOREIGN KEY (qualificacoes) REFERENCES public.id_qualificacoes(qualificacoes)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.estabelecimentos (
    cnpj_basico CHARACTER(8) NOT NULL,
    cnpj_ordem CHARACTER(4) NOT NULL,
    cnpj_dv CHARACTER(2) NOT NULL,
    identificador INTEGER NOT NULL,
    nome_fantasia CHARACTER VARYING(200),
    situacao_cadastral INTEGER NOT NULL,
    data_situacao_cadastral CHARACTER VARYING(8),
    motivo_situacao_cadastral INTEGER NOT NULL,
    nome_cidade_exterior CHARACTER VARYING(200),
    cod_pais INTEGER NOT NULL,
    data_inicio_atividade CHARACTER VARYING(8),
    cnae_fiscal_principal INTEGER NOT NULL,
    cnae_fiscal_secundario CHARACTER VARYING(800),
    tipo_logradouro CHARACTER VARYING(200),
    logradouro CHARACTER VARYING(200),
    numero CHARACTER VARYING(200),
    complemento CHARACTER VARYING(200),
    bairro CHARACTER VARYING(200),
    cep CHARACTER VARYING(200),
    uf CHARACTER VARYING(200),
    cod_municipio INTEGER NOT NULL,
    ddd1 CHARACTER VARYING(200),
    telefone1 CHARACTER VARYING(200),
    ddd2 CHARACTER VARYING(200),
    telefone2 CHARACTER VARYING(200),
    ddd_fax CHARACTER VARYING(200),
    fax CHARACTER VARYING(200),
    email CHARACTER VARYING(200),
    situacao_especial CHARACTER VARYING(200),
    data_situacao_especial CHARACTER VARYING(8),
    CONSTRAINT identificador FOREIGN KEY (identificador) REFERENCES public.id_identificador(identificador),
    CONSTRAINT situacao_cadastral FOREIGN KEY (situacao_cadastral) REFERENCES public.id_situacao_cadastral(situacao_cadastral),
    CONSTRAINT cod_pais FOREIGN KEY (cod_pais) REFERENCES public.paises(cod_pais),
    CONSTRAINT cod_municipio FOREIGN KEY (cod_municipio) REFERENCES public.municipios(cod_municipio),
    CONSTRAINT cnae_fiscal_principal FOREIGN KEY (cnae_fiscal_principal) REFERENCES public.id_cnae_fiscal_principal(cnae_fiscal_principal),
    CONSTRAINT motivo_situacao_cadastral FOREIGN KEY (motivo_situacao_cadastral) REFERENCES public.id_motivo_situacao_cadastral(motivo_situacao_cadastral)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.simples (
    cnpj_basico CHARACTER(8) NOT NULL,
    opcao_simples INTEGER NOT NULL,
    data_opcao_simples CHARACTER VARYING(8),
    data_exclusao_simples CHARACTER VARYING(8),
    opcao_mei INTEGER NOT NULL,
    data_opcao_mei CHARACTER VARYING(8),
    data_exclusao_mei CHARACTER VARYING(8),
    CONSTRAINT opcao_simples FOREIGN KEY (opcao_simples) REFERENCES public.id_opcao_simples(opcao_simples),
    CONSTRAINT opcao_mei FOREIGN KEY (opcao_mei) REFERENCES public.id_opcao_mei(opcao_mei)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.socios (
    cnpj_basico CHARACTER(8) NOT NULL,
    identificador_socio INTEGER NOT NULL,
    nome_socio CHARACTER VARYING(200),
    cnpj_cpf_socio CHARACTER VARYING(200),
    qualificacoes INTEGER NOT NULL,
    data_entrada_sociedade CHARACTER VARYING(8),
    cod_pais INTEGER NOT NULL,
    representante_legal CHARACTER VARYING(200),
    nome_representante CHARACTER VARYING(200),
    qualificacao_representante CHARACTER VARYING(200),
    faixa_etaria CHARACTER VARYING(200),
    CONSTRAINT identificador_socio FOREIGN KEY (identificador_socio) REFERENCES public.id_identificador_socio(identificador_socio),
    CONSTRAINT cod_pais FOREIGN KEY (cod_pais) REFERENCES public.paises(cod_pais),
    CONSTRAINT qualificacoes FOREIGN KEY (qualificacoes) REFERENCES public.id_qualificacoes(qualificacoes)
);
----------------------------------------------------------------------------------------------------
INSERT INTO public.id_porte_empresa
VALUES
    (0, 'NAO INFORMADO'),
    (1, 'MICRO EMPRESA'),
    (3, 'EMPRESA DE PEQUENO PORTE'),
    (5, 'DEMAIS');

INSERT INTO public.id_identificador
VALUES
    (0, 'VAZIO'),
    (1, 'MATRIZ'),
    (2, 'FILIAL');

INSERT INTO public.id_situacao_cadastral
VALUES
    (1, 'NULA'),
    (2, 'ATIVA'),
    (3, 'SUSPENSA'),
    (4, 'INAPTA'),
    (8, 'BAIXADA');

INSERT INTO public.id_opcao_simples
VALUES
    (1, 'SIM'),
    (0, 'NAO'),
    (2, 'OUTROS');

INSERT INTO public.id_opcao_mei
VALUES
    (1, 'SIM'),
    (0, 'NAO'),
    (2, 'OUTROS');

INSERT INTO public.id_identificador_socio
VALUES
    (0, 'NENHUM'),
    (1, 'PESSOA JURIDICA'),
    (2, 'PESSOA FISICA'),
    (3, 'ESTRANGEIRO');