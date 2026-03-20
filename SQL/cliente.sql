-- Query utilizada pelo migrador para extrair os Clientes da Tabela CADASTRO
-- Utiliza Joins com FISCAL, ENDERECO, IBCGE e PAISES para consolidar os dados essenciais.
SELECT
    CASE 
        WHEN c.CNPJ_CPF IS NOT NULL AND TRIM(c.CNPJ_CPF) <> '' THEN LEFT(REPLACE(REPLACE(REPLACE(c.CNPJ_CPF, '.', ''), '-', ''), '/', ''), 18)
        ELSE CAST(c.ID AS CHAR(18))
    END AS id,
    c3.DESCRICAO AS atividade,
    CASE WHEN c.STATUS = 1 THEN 1 ELSE 0 END AS ativo,
    LEFT(c.AVISO, 50) AS aviso,
    LEFT(ce.BAIRRO, 30) AS bairro,
    LEFT(ce.CELULAR, 20) AS celular,
    ce.CEP AS cep,
    LEFT(uf.MUNICIPIO, 50) AS cidade,
    c.ID AS codigo,
    c.ID AS codigomigrado,
    LEFT(ce.COMPLEMENTO, 20) AS complemento,
    LEFT(ce.CONTATO, 20) AS comprador,
    c.DATA_CADASTRO AS datacadastro,
    LEFT(c.E_MAIL, 50) AS email,
    LEFT(C.FANTASIA, 30) AS fantasia,
    LEFT(ce.FONE, 30) AS fone,
    uf.CODIGO_MUNICIPIO AS idcidade,
    1 AS idloja,
    p.CODIGO AS idpais,
    'C' AS idtipo,
    LEFT(c2.RAZAO_SOCIAL, 30) AS idvendedor,
    CASE 
        WHEN c.INSCRICAO_RG = 'INSENTO' THEN NULL
        WHEN CHAR_LENGTH(c.CNPJ_CPF) = 18 THEN LEFT(REPLACE(REPLACE(REPLACE(c.INSCRICAO_RG, '.', ''), '-', ''), '/', ''), 20)
        ELSE NULL
    END AS ie,
    LEFT(ce.LOGRADOURO, 80) AS logradouro,
    '1' AS lojaorigem,
    c.OUTRAS_OBSERVACOES AS memo2,
    LEFT(c.RAZAO_SOCIAL, 80) AS nome,
    ce.NUMERO AS numero,
    c.OBSERVACAO AS observacao,
    CASE WHEN c.FISICA_JURIDICA = 'J' THEN 1 ELSE 0 END AS pessoajuridica,
    CASE 
        WHEN CHAR_LENGTH(c.CNPJ_CPF) < 18 AND c.INSCRICAO_RG <> '' THEN LEFT(c.INSCRICAO_RG, 20)
        ELSE NULL
    END AS rg,
    LEFT(c.SITE, 100) AS site,
    CASE WHEN c.STATUS = 1 THEN 'ATIVA' ELSE 'INATIVA' END AS status,
    CASE
        WHEN c.INSCRICAO_SUFRAMA IS NOT NULL AND c.INSCRICAO_SUFRAMA <> '' THEN LEFT(c.INSCRICAO_SUFRAMA, 20)
        ELSE NULL
    END AS suframa,
    c.FISICA_JURIDICA AS tipo,
    ce.UF AS uf,
    CASE WHEN CHAR_LENGTH(c.CNPJ_CPF) < 18 THEN LEFT(c.CNPJ_CPF, 18) END AS cpf,
    LEFT(REPLACE(REPLACE(REPLACE(REPLACE(ce.WHATSAPP, '(', ''), ')', ''), '-', ''), ' ', ''), 11) AS whatsapp,
    c.DATA_NASCIMENTO AS aniversario 
FROM CADASTRO c
LEFT JOIN CADASTRO c2 ON c.FK_VENDEDOR = c2.ID 
LEFT JOIN CADASTRO_ENDERECO ce ON ce.ID = (SELECT MAX(CE_INT.ID) FROM CADASTRO_ENDERECO CE_INT WHERE CE_INT.FK_CADASTRO = c.ID)
LEFT JOIN UF_MUNICIPIO_IBGE uf ON ce.FK_UF_MUNICIPIO_IBGE = uf.ID 	
LEFT JOIN PAISES p ON ce.FK_PAISES = p.ID 
LEFT JOIN CADASTRO_FISCAL cf ON c.ID = cf.FK_CADASTRO
LEFT JOIN CNAE c3 ON cf.FK_CNAE = c3.ID
WHERE c.CHK_CLIENTE = 'S'
