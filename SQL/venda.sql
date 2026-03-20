-- Query utilizada para extrair o cabeçalho das Vendas (PEDIDO)
-- Observação importante: Filtra pelas Empresas corretas e Pedidos de tipos '1' e '5' (Cancelados contam).
SELECT
    V.ID AS idvenda,
    V.DATA_EMISSAO AS data,
    CASE
        WHEN C.CNPJ_CPF IS NOT NULL AND C.CNPJ_CPF <> '' THEN LEFT(REPLACE(REPLACE(REPLACE(C.CNPJ_CPF, '.', ''), '/', ''), '-', ''), 14)
        ELSE CAST(C.ID AS VARCHAR(14))
    END AS idcliente,
    V.FK_VENDEDOR AS idusuario,
    'S' AS concluido,
    '0' AS idterminal,
    '1' AS ncaixa,
    V.TOTAL_PEDIDO AS valor,
    CAST(NULL AS DECIMAL(10,2)) AS troco,
    COALESCE(V.VALOR_DESCONTO, 0) AS desconto,
    V.FRETE_VALOR AS taxa,
    V.ACRESCIMO AS acrescimo,
    COALESCE(CI.IDPLANO, 1) AS idplano, -- Será re-sobrescrito no Python se houver parcelas múltiplas mapeadas
    CASE
        WHEN V.FATURADO = 'S' THEN '3' 
        WHEN V.CANCELADO = 'T' THEN '2'
        ELSE '1'
    END AS status,
    V.OBSERVACAO AS observacaointerna,
    V.OBS_FISCAL AS observacaocliente
FROM 
    PEDIDO V
LEFT JOIN 
    CADASTRO C ON C.ID = V.FK_CADASTRO
LEFT JOIN (
    -- Subquery para capturar o primeiro TIPO de pagamento associado diretamente no CAIXA em vendas à vista como fallback de idplano
    SELECT
        FK_PEDIDO,
        MIN(FK_TIPO_PAGAMENTO) AS IDPLANO
    FROM CAIXA_ITEM
    GROUP BY FK_PEDIDO
) CI ON CI.FK_PEDIDO = V.ID
WHERE 
    V.FK_EMPRESA = {fk_empresa} AND 
    (V.FK_TIPO_PEDIDO = '1' OR V.FK_TIPO_PEDIDO = '5')
