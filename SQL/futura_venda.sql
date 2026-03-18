-- Migradr Venda
SELECT
	-- idvenda (VEM DO DESTINO GERADO AUTOINCREMENTO DA TABELA VENDA, DEVE SER RECUPERADO E REPASSADO PARA OS ITENS E PARCELAS ASSOCIADOS A ESSA VENDA) -- int
	-- idloja (VEM DO MIGRADOR CAMPO IDLOJA QUE DEVE SER SETADO NO INICIO DO MIGRADOR) -- smallint
	'V' AS operacao, -- char(1)
	ROW_NUMBER() OVER(PARTITION BY V.DATA_EMISSAO ORDER BY V.DATA_HORA_ABERTURA) AS romaneio, -- int
	-- idterminal           -- smallint
	CASE 
		WHEN SU.STATUS = 1 THEN SU.ID
	END AS idusuario, -- smallint
	CASE
            WHEN CCLI.CNPJ_CPF IS NOT NULL AND CCLI.CNPJ_CPF <> '' THEN LEFT(REPLACE(REPLACE(REPLACE(CCLI.CNPJ_CPF, '.', ''), '/', ''), '-', ''), 14)
            ELSE CAST(CCLI.ID AS VARCHAR(14))
        END AS idcliente, -- char(14)
	V.DATA_EMISSAO AS datacadastro, -- datetime
	T.QUANTIDADE AS quantidade, -- decimal(10,2)
	V.TOTAL_PRODUTO AS total, -- decimal(15,2)
	V.TOTAL_PEDIDO AS liquido, -- decimal(15,2)
    COALESCE(FIN.TOTAL_PARCELAS, 0) AS parcela, -- smallint
	-- prazo           -- smallint
	V.NRO_PEDIDO AS pedido, -- int
	CASE
            WHEN V.STATUS = 3 THEN 'C'
            WHEN V.STATUS = 2 THEN 'F'
            WHEN V.STATUS = 4 THEN 'N'
        END AS status, -- char(1)
	CI.IDPLANO, -- smallint
	LEFT(CVEND.RAZAO_SOCIAL, 30) AS idvendedor, -- char(30)
	-- idrepresentante           -- char(14)
	LEFT(V.OBSERVACAO, 100) AS obs, -- varchar(100)
	-- motivo           -- varchar(100)
	-- local           -- smallint
	-- quitado           -- tinyint(1)
	-- atraso           -- smallint
	-- log           -- smallint
	-- aberto           -- char(1)
	-- juntado           -- int
	V.DATA_HORA_FECHAMENTO AS data, -- date
	V.DATA_HORA_FECHAMENTO AS hora, -- time
	-- troca           -- decimal(10,2)
	-- vfrete           -- decimal(10,2)
	-- online           -- tinyint(1)
	-- varejo           -- tinyint(1)
	-- fidelidade           -- decimal(10,2)
	-- devolucao           -- decimal(10,2)
	-- idterminalalteracao           -- smallint
	-- idautorizacao           -- smallint
	-- idusuarioalteracao           -- smallint
	-- orderidvtex           -- varchar(100)
	-- sequencevtex           -- int
	-- idcompra           -- int
	-- idpedido           -- int
	NF.NRO_NOTA AS nnf, -- int
	-- emissor           -- char(18)
	-- idpacking           -- varchar(100)
	-- conferido           -- tinyint(1)
	-- marketplace           -- varchar(100)
	-- idemitente           -- int
	-- postagem           -- varchar(50)
	-- desconto           -- decimal(10,2)
	-- troco           -- decimal(10,2)
	-- valorpago           -- decimal(10,2)
	-- checkout           -- tinyint(1)
	V.DATA_HORA_ABERTURA AS created, -- timestamp
	V.DATA_HORA_ABERTURA AS updated, -- timestamp
	-- idtransportadora           -- char(18)
	-- modalidadefrete           -- tinyint
	-- pesol           -- decimal(5,2)
	-- pesob           -- decimal(5,2)
	-- volume           -- tinyint
	CASE
		WHEN CI.FK_CARTAO IS NOT NULL AND CI.FK_CARTAO > 0 THEN C.DESCRICAO
		ELSE TP.DESCRICAO 
	END AS especie -- varchar(50)
	-- representante           -- varchar(100)
FROM PEDIDO V
    LEFT JOIN CADASTRO CCLI ON CCLI.ID = V.FK_CADASTRO
    LEFT JOIN CADASTRO CVEND ON CVEND.ID = V.FK_VENDEDOR
    LEFT JOIN NOTA_FISCAL NF ON NF.FK_PEDIDO = V.ID
    -- Subconsulta de Itens (Garante 1 linha)
    LEFT JOIN (
        SELECT
            FK_PEDIDO,
            SUM(QUANTIDADE) AS QUANTIDADE
        FROM PEDIDO_ITEM       
        GROUP BY FK_PEDIDO) T ON T.FK_PEDIDO = V.ID
    -- Subconsulta de Caixa (Garante 1 linha)
    LEFT JOIN (
        SELECT 
            FK_PEDIDO,
            MIN(FK_TIPO_PAGAMENTO) AS IDPLANO, -- MIN para garantir uma única forma se houver conflito
            MIN(FK_CARTAO) AS FK_CARTAO
        FROM CAIXA_ITEM
        GROUP BY FK_PEDIDO
    ) CI ON CI.FK_PEDIDO = V.ID
    -- NOVA Subconsulta de Parcelas (Garante 1 linha e conta as parcelas)
    LEFT JOIN (
        SELECT 
            C_INT.FK_PEDIDO,
            COUNT(CP_INT.ID) AS TOTAL_PARCELAS
        FROM CONTA C_INT
        JOIN CONTA_PARCELA CP_INT ON CP_INT.FK_CONTA = C_INT.ID
        GROUP BY C_INT.FK_PEDIDO
    ) FIN ON FIN.FK_PEDIDO = V.ID
    LEFT JOIN SYS_USUARIO SU ON SU.ID = V.FK_USUARIO_PED 
    LEFT JOIN TIPO_PAGAMENTO TP ON TP.ID = CI.IDPLANO  
    LEFT JOIN CARTAO C ON C.ID = CI.FK_CARTAO
    WHERE V.FK_TIPO_PEDIDO = '1'
      AND V.STATUS <> '1'
      AND V.FK_EMPRESA = 4839