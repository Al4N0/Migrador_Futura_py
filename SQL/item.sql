-- Query utilizada para extrair todos os Itens (PEDIDO_ITEM) simultaneamente para memória (Otimização massiva).
-- Eles são mantidos na RAM e consultados na hora de inserir o respectivo cabeçalho da Venda.
SELECT
    I.FK_PEDIDO AS fk_pedido,
    I.ID AS idauxiliar,
    CASE
        WHEN C.CNPJ_CPF IS NOT NULL AND C.CNPJ_CPF <> '' THEN LEFT(REPLACE(REPLACE(REPLACE(C.CNPJ_CPF, '.', ''), '/', ''), '-', ''), 14)
        ELSE CAST(C.ID AS VARCHAR(14))
    END AS idcliente, 
    P.FK_VENDEDOR AS idusuario,
    CASE
        WHEN PCB.CODIGO_BARRA IS NULL OR PCB.CODIGO_BARRA = '' THEN LEFT(PROD.ID, 20)
        ELSE LEFT(PCB.CODIGO_BARRA, 20)
    END AS idproduto,
    I.QUANTIDADE AS quantidade,
    COALESCE(I.PRECO_CUSTO, 0) AS custoproduto,
    I.VALOR_UNITARIO AS valorbruto,
    COALESCE(I.VALOR_DESCONTO, 0) AS valordesconto,
    I.VALOR_UNITARIO - COALESCE(I.VALOR_DESCONTO, 0) + COALESCE(I.VALOR_ACRESCIMO, 0) AS valorliquido,
    P.DATA_EMISSAO AS data,
    '*' AS tamanho,
    '*' AS cor,
    CASE WHEN P.FATURADO = 'S' THEN '3' WHEN P.CANCELADO = 'T' THEN '2' ELSE '1' END AS status
FROM PEDIDO_ITEM I
JOIN PEDIDO P ON P.ID = I.FK_PEDIDO
LEFT JOIN PRODUTO PROD ON PROD.ID = I.FK_PRODUTO
LEFT JOIN CADASTRO C ON C.ID = P.FK_CADASTRO
LEFT JOIN PRODUTO_CODIGO_BARRA PCB ON PCB.FK_PRODUTO = PROD.ID
WHERE P.FK_EMPRESA = {fk_empresa} AND (P.FK_TIPO_PEDIDO = '1' OR P.FK_TIPO_PEDIDO = '5')
