-- CREATE DATABASE public_cnpj
--     WITH
--     OWNER = postgres
--     ENCODING = 'UTF8'
--     CONNECTION LIMIT = -1
--     IS_TEMPLATE = False;

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
DROP TABLE IF EXISTS public.empresas;
DROP TABLE IF EXISTS public.estabelecimentos;
DROP TABLE IF EXISTS public.simples;
DROP TABLE IF EXISTS public.socios;
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_porte_empresa (
    porte_empresa CHARACTER(2) NOT NULL,
    descricao CHARACTER VARYING(30) NOT NULL,
    PRIMARY KEY (porte_empresa)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_identificador (
    identificador INTEGER NOT NULL,
    descricao CHARACTER(6) NOT NULL,
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
    opcao_simples CHARACTER VARYING(9) NOT NULL,
    descricao CHARACTER VARYING(6) NOT NULL,
    PRIMARY KEY (opcao_simples)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_opcao_mei (
    opcao_mei CHARACTER VARYING(9) NOT NULL,
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
    cod_pais CHARACTER VARYING(300) NOT NULL,
    pais CHARACTER VARYING(300) NOT NULL,
    PRIMARY KEY (cod_pais)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.municipios (
    cod_municipio CHARACTER VARYING(300) NOT NULL,
    descricao CHARACTER VARYING(300) NOT NULL,
    PRIMARY KEY (cod_municipio)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_qualificacoes (
    qualificacoes CHARACTER(2) NOT NULL,
    descricao CHARACTER VARYING(300) NOT NULL,
    PRIMARY KEY (qualificacoes)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_natureza_juridica (
    natureza_juridica CHARACTER(4) NOT NULL,
    descricao CHARACTER VARYING(300) NOT NULL,
    PRIMARY KEY (natureza_juridica)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_cnae_fiscal_principal (
    cnae_fiscal_principal CHARACTER VARYING(300) NOT NULL,
    descricao CHARACTER VARYING(300) NOT NULL,
    PRIMARY KEY (cnae_fiscal_principal)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.empresas (
    cnpj_basico CHARACTER(8) NOT NULL,
    razao_social CHARACTER VARYING(300),
    natureza_juridica CHARACTER(4) NOT NULL,
    qualificacoes CHARACTER(2) NOT NULL,
    capital_social NUMERIC,
    porte_empresa CHARACTER(2) NOT NULL,
    ente_federativo_responsavel CHARACTER VARYING(300),
    CONSTRAINT natureza_juridica FOREIGN KEY (natureza_juridica) REFERENCES public.id_natureza_juridica(natureza_juridica),
    CONSTRAINT porte_empresa FOREIGN KEY (porte_empresa) REFERENCES public.id_porte_empresa(porte_empresa),
    CONSTRAINT qualificacoes FOREIGN KEY (qualificacoes) REFERENCES public.id_qualificacoes(qualificacoes)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.id_motivo_situacao_cadastral (
    motivo_situacao_cadastral CHARACTER(2) NOT NULL,
    descricao CHARACTER VARYING(300),
    PRIMARY KEY (motivo_situacao_cadastral)
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
    motivo_situacao_cadastral CHARACTER VARYING(300) NOT NULL,
    nome_cidade_exterior CHARACTER VARYING(300),
    cod_pais CHARACTER VARYING(300) NOT NULL,
    data_inicio_atividade CHARACTER VARYING(300),
    cnae_fiscal_principal CHARACTER VARYING(300) NOT NULL,
    cnae_fiscal_secundario CHARACTER VARYING(300),
    tipo_logradouro CHARACTER VARYING(300),
    logradouro CHARACTER VARYING(300),
    numero CHARACTER VARYING(300),
    complemento CHARACTER VARYING(300),
    bairro CHARACTER VARYING(300),
    cep CHARACTER VARYING(300),
    uf CHARACTER VARYING(300),
    cod_municipio CHARACTER VARYING(300) NOT NULL,
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
    CONSTRAINT situacao_cadastral FOREIGN KEY (situacao_cadastral) REFERENCES public.id_situacao_cadastral(situacao_cadastral),
    CONSTRAINT cod_pais FOREIGN KEY (cod_pais) REFERENCES public.paises(cod_pais),
    CONSTRAINT cod_municipio FOREIGN KEY (cod_municipio) REFERENCES public.municipios(cod_municipio),
    CONSTRAINT cnae_fiscal_principal FOREIGN KEY (cnae_fiscal_principal) REFERENCES public.id_cnae_fiscal_principal(cnae_fiscal_principal),
    CONSTRAINT motivo_situacao_cadastral FOREIGN KEY (motivo_situacao_cadastral) REFERENCES public.id_motivo_situacao_cadastral(motivo_situacao_cadastral)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.simples (
    cnpj_basico CHARACTER(8) NOT NULL,
    opcao_simples CHARACTER VARYING(9) NOT NULL,
    data_opcao_simples CHARACTER VARYING(300),
    data_exclusao_simples CHARACTER VARYING(300),
    opcao_mei CHARACTER VARYING(9) NOT NULL,
    data_opcao_mei CHARACTER VARYING(300),
    data_exclusao_mei CHARACTER VARYING(300),
    CONSTRAINT opcao_simples FOREIGN KEY (opcao_simples) REFERENCES public.id_opcao_simples(opcao_simples),
    CONSTRAINT opcao_mei FOREIGN KEY (opcao_mei) REFERENCES public.id_opcao_mei(opcao_mei)
);
----------------------------------------------------------------------------------------------------
CREATE TABLE public.socios (
    cnpj_basico CHARACTER(8) NOT NULL,
    identificador_socio INTEGER NOT NULL,
    nome_socio CHARACTER VARYING(300),
    cnpj_cpf_socio CHARACTER VARYING(300),
    qualificacoes CHARACTER(2) NOT NULL,
    data_entrada_sociedade CHARACTER VARYING(300),
    cod_pais CHARACTER VARYING(300) NOT NULL,
    representante_legal CHARACTER VARYING(300),
    nome_representante CHARACTER VARYING(300),
    qualificacao_representante CHARACTER VARYING(300),
    faixa_etaria CHARACTER VARYING(300),
    CONSTRAINT identificador_socio FOREIGN KEY (identificador_socio) REFERENCES public.id_identificador_socio(identificador_socio),
    CONSTRAINT cod_pais FOREIGN KEY (cod_pais) REFERENCES public.paises(cod_pais),
    CONSTRAINT qualificacoes FOREIGN KEY (qualificacoes) REFERENCES public.id_qualificacoes(qualificacoes)
);
----------------------------------------------------------------------------------------------------
INSERT INTO public.id_porte_empresa
VALUES
    ('00', 'NAO INFORMADO'),
    ('01', 'MICRO EMPRESA'),
    ('03', 'EMPRESA DE PEQUENO PORTE'),
    ('05', 'DEMAIS');

INSERT INTO public.id_identificador
VALUES
    (1, 'MATRIZ'),
    (2, 'FILIAL');

INSERT INTO public.id_situacao_cadastral
VALUES
    ('01', 'NULA'),
    ('2', 'ATIVA'),
    ('3', 'SUSPENSA'),
    ('4', 'INAPTA'),
    ('08', 'BAIXADA');

INSERT INTO public.id_opcao_simples
VALUES
    ('S', 'SIM'),
    ('N', 'NAO'),
    ('EM BRANCO', 'OUTROS');

INSERT INTO public.id_opcao_mei
VALUES
    ('S', 'SIM'),
    ('N', 'NAO'),
    ('EM BRANCO', 'OUTROS');

INSERT INTO public.id_identificador_socio
VALUES
    (1, 'PESSOA JURIDICA'),
    (2, 'PESSOA FISICA'),
    (3, 'ESTRANGEIRO');