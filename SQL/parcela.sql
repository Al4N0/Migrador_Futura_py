-- Query utilizada para obter as Formas e Valores financeiros atrelados aos Pedidos.
-- Coleta de forma unificada: Parcelas Faturadas (CONTA_PARCELA) e Pagamentos à vista (CAIXA_ITEM).
SELECT * FROM (

    -- 1. VENDAS FATURADAS (Lançadas como contas a receber e rateadas em parcelas)
    SELECT
        P.ID AS fk_pedido,
        ROW_NUMBER() OVER (PARTITION BY P.ID ORDER BY CP.DATA_VENCIMENTO, CP.ID) AS idparcela,
        CP.ID AS taxid,
        CASE
            WHEN C2.CNPJ_CPF IS NOT NULL AND C2.CNPJ_CPF <> '' THEN LEFT(REPLACE(REPLACE(REPLACE(C2.CNPJ_CPF, '.', ''), '/',''),'-',''), 14)
            ELSE CAST(C2.ID AS VARCHAR(14))
        END AS idcliente, 
        C.FK_USUARIO AS idusuario,
        P.DATA_EMISSAO AS data,
        CO.DOCUMENTO AS documento,
        CASE WHEN CO.FK_CARTAO > 0 THEN CA.DESCRICAO ELSE '' END AS forma_pagamento,
        COUNT(CP2.ID) AS total_parcelas, -- Permite descobrir a quantidade multiplicadora (ex: "CREDITO 4X") baseando na conta mãe.
        CP.VALOR_PARCELA AS valor,
        CP.DATA_VENCIMENTO AS vencimento,
        CASE WHEN CP.VLR_PAGO > 0 THEN CP.DATA_PAGTO ELSE NULL END AS datapagamento
    FROM CONTA_PARCELA CP
    JOIN CONTA CO ON CO.ID = CP.FK_CONTA
    JOIN PEDIDO P ON P.ID = CO.FK_PEDIDO
    LEFT JOIN CADASTRO C ON C.ID = P.FK_CADASTRO
    LEFT JOIN CADASTRO C2 ON C2.ID = P.FK_CADASTRO
    LEFT JOIN CARTAO CA ON CA.ID = CO.FK_CARTAO
    LEFT JOIN CONTA_PARCELA CP2 ON CP2.FK_CONTA = CO.ID
    WHERE P.FK_EMPRESA = {fk_empresa} AND (P.FK_TIPO_PEDIDO = '1' OR P.FK_TIPO_PEDIDO = '5')
    GROUP BY P.ID, CP.ID, C2.CNPJ_CPF, C2.ID, C.FK_USUARIO, P.DATA_EMISSAO, CO.DOCUMENTO, CO.FK_CARTAO, CA.DESCRICAO, CP.VALOR_PARCELA, CP.DATA_VENCIMENTO, CP.VLR_PAGO, CP.DATA_PAGTO

    UNION ALL

    -- 2. VENDAS À VISTA REGISTRADAS DIRETAMENTE NO CAIXA (Liquidadas no balcão sem fatura)
    SELECT
        P.ID AS fk_pedido,
        1 AS idparcela,
        CI.ID AS taxid,
        CASE
            WHEN C2.CNPJ_CPF IS NOT NULL AND C2.CNPJ_CPF <> '' THEN LEFT(REPLACE(REPLACE(REPLACE(C2.CNPJ_CPF, '.', ''), '/',''),'-',''), 14)
            ELSE CAST(C2.ID AS VARCHAR(14))
        END AS idcliente, 
        P.FK_USUARIO AS idusuario,
        P.DATA_EMISSAO AS data,
        CAST(P.ID AS VARCHAR(255)) AS documento,
        CASE WHEN CI.FK_CARTAO > 0 THEN CA.DESCRICAO ELSE TP.DESCRICAO END AS forma_pagamento,
        1 AS total_parcelas,
        CI.VALOR AS valor,
        P.DATA_EMISSAO AS vencimento,
        P.DATA_EMISSAO AS datapagamento
    FROM CAIXA_ITEM CI
    JOIN PEDIDO P ON P.ID = CI.FK_PEDIDO
    LEFT JOIN CADASTRO C ON C.ID = P.FK_CADASTRO
    LEFT JOIN CADASTRO C2 ON C2.ID = P.FK_CADASTRO
    LEFT JOIN CARTAO CA ON CA.ID = CI.FK_CARTAO
    LEFT JOIN TIPO_PAGAMENTO TP ON TP.ID = CI.FK_TIPO_PAGAMENTO
    WHERE P.FK_EMPRESA = {fk_empresa} AND (P.FK_TIPO_PEDIDO = '1' OR P.FK_TIPO_PEDIDO = '5')

)
ORDER BY fk_pedido, idparcela
