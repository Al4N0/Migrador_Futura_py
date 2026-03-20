SELECT
     -- idloja (VEM DO MIGRADOR CAMPO IDLOJA QUE DEVE SER SETADO NO INICIO DO MIGRADOR) -- smallint
    CASE
         WHEN PCB.CODIGO_BARRA IS NULL OR PCB.CODIGO_BARRA = '' THEN LEFT(P.ID, 20)
         ELSE LEFT(PCB.CODIGO_BARRA, 20)
    END AS id,  -- char(20)
     -- altura                          -- smallint
     -- blocok                          -- decimal(10,4)
     -- boca                          -- decimal(5,2)
     -- braco                          -- decimal(5,2)
     -- busto                          -- decimal(5,2)
     -- cadconsumo                          -- decimal(5,2)
     -- cadlargura                          -- decimal(5,2)
     LEFT(PF.NRO_FABRICANTE, 45) AS codigofornecedor, -- varchar(45)
     -- comp                          -- decimal(5,2)
     -- comprimento                          -- smallint
     -- cintura                          -- decimal(5,2)
     -- complementar                          -- varchar(255)
     -- composicao                          -- text
     -- conversao                          -- decimal(10,4)
     -- corte                          -- char(20)
     -- cortepiloto                          -- char(20)
     -- costa                          -- decimal(5,2)
     COALESCE(P.CUSTO, 0) AS custo, -- decimal(10,4)
     -- customaterial                          -- decimal(10,4)
     -- custoproducao                          -- decimal(10,4)
     P.DATA_CADASTRO AS datacadastro, -- date
     -- diametro                          -- smallint
     LEFT(P.DESCRICAO, 120) AS descricao, -- char(120)
     -- descricaoseo                          -- text
     -- edesc                          -- varchar(255)
     -- embalagem                          -- varchar(50)
     -- entrada                          -- decimal(10,4)
     -- entrepernas                          -- decimal(5,2)
     -- fornecedororigem                          -- varchar(100)
     -- gancho                          -- decimal(5,2)
     -- icmscst                          -- char(3)
     -- idcategoria                          -- char(20)
     -- idcategoriatuddu                          -- smallint
     -- idcolecao                          -- char(50)
     -- idcorfiltrotuddu                          -- smallint
     -- idfabricacao                          -- smallint
     CASE
        WHEN C.CNPJ_CPF IS NOT NULL AND C.CNPJ_CPF <> '' THEN LEFT(REPLACE(REPLACE(REPLACE(C.CNPJ_CPF, '.', ''), '/', ''), '-', ''), 14)
        ELSE CAST(C.ID AS VARCHAR(14))
     END AS idfornecedor, -- char(14)     
     -- idmarca                          -- char(20)
     -- idmarcatuddu                          -- varchar(30)
     -- idmaterial                          -- char(20)
     -- idmodelo                          -- char(20)
     -- idmodelotuddu                          -- smallint
     -- idorigem                          -- smallint
     -- idplataforma                          -- int
     -- idsubcategoria                          -- char(20)
     -- idsubcategoriatuddu                          -- smallint
     -- idtecidotuddu                          -- smallint
     -- idtipo                          -- varchar(30)
     -- idtuddu                          -- varchar(30)
     -- idusuario                          -- smallint
     -- idvtex                          -- int
     -- imagem1                          -- varchar(255)
     -- imagem2                          -- varchar(255)
     -- imagem3                          -- varchar(255)
     -- imagem4                          -- varchar(255)
     -- imagem5                          -- varchar(255)
     -- imagem6                          -- varchar(255)
     -- integrado                          -- tinyint(1)
     -- iscompleto                          -- tinyint(1)
     -- isdestaque                          -- tinyint(1)
     -- isshowroom                          -- tinyint(1)
     -- lancamento                          -- date
     -- largura                          -- smallint
     -- local                          -- varchar(255)
     -- migrado                          -- tinyint(1)
     -- modelagem                          -- char(20)
     -- modelotamanho                          -- varchar(20)
     -- modeloaltura                          -- decimal(5,2)
     -- modelopeso                          -- decimal(5,2)
     -- nfci                          -- varchar(45)
     LEFT(CF.CLASSIFICACAO, 8) AS ncm, -- char(8)
     -- nforigem                          -- int
     P.OBSERVACAO AS obs, -- text
     -- ombro                          -- decimal(5,2)
     -- palavrachaveseo                          -- varchar(255)
     -- pdf                          -- varchar(255)
     -- pdf2                          -- varchar(255)
     -- pedido                          -- decimal(10,4)
     -- pesol                          -- decimal(6,3)
     -- pesob                          -- decimal(6,3)
     -- piloto                          -- char(20)
     -- pilotoconsumo                          -- decimal(5,2)
     -- pilotolargura                          -- decimal(5,2)
     -- ppreco1                          -- decimal(10,4)
     -- ppreco2                          -- decimal(10,4)
     COALESCE(P1.VALOR, 0) AS preco1, -- decimal(10,4)
     -- preco2                          -- decimal(10,4)
     COALESCE(P2.VALOR, 0) AS preco3, -- decimal(10,4)
     -- preco4                          -- decimal(10,4)
     -- preco5                          -- decimal(10,4)
     -- precoinicial                          -- decimal(10,4)
     -- promocaopercentual                          -- decimal(5,2)
     -- precosite                          -- decimal(10,4)
     -- precotuddu                          -- decimal(10,4)
     -- promocao                          -- tinyint(1)
     -- quadril                          -- decimal(5,2)
     -- saida                          -- decimal(10,4)
     -- saldo                          -- decimal(15,4)
     -- selecionado                          -- tinyint(1)
     CASE
        WHEN P.STATUS = '0' THEN 'ATIVO'
        ELSE 'DELETED'
    END AS status, -- char(10)
     'P' AS tipo, -- char(1)
     -- tituloseo                          -- varchar(255)
     -- ultimaalteracao                          -- datetime
     LEFT(PU.SIGLA, 6) AS unidade -- char(6)
     -- vesti                          -- tinyint(1)
     -- tabelamedida                          -- varchar(255)
     -- tag                          -- varchar(255)
     -- tuddu                          -- tinyint(1)
     -- updated                          -- timestamp
     -- preco1p                          -- decimal(10,4)
     -- preco2p                          -- decimal(10,4)
     -- preco3p                          -- decimal(10,4)
     -- preco4p                          -- decimal(10,4)
     -- preco5p                          -- decimal(10,4)
     -- dataultimavenda                          -- date
     -- idterminal                          -- smallint
     -- st                          -- tinyint(1)
     -- mva                          -- decimal(5,2)
     -- datamodelagem                          -- date
     -- dataproducao                          -- date
     -- cest                          -- varchar(7)
     -- precominimo                          -- decimal(10,4)
FROM PRODUTO P
LEFT JOIN PRODUTO_UNIDADE PU ON PU.ID = P.FK_PRODUTO_UNIDADE
LEFT JOIN CLASSIFICACAO_FISCAL CF ON CF.ID = P.FK_CLASSIFICACAO_FISCAL
LEFT JOIN PRODUTO_CODIGO_BARRA PCB ON PCB.FK_PRODUTO = P.ID
LEFT JOIN PRODUTO_FORNECEDOR PF ON PF.FK_PRODUTO = P.ID
LEFT JOIN CADASTRO C ON C.ID = PF.FK_FORNECEDOR
LEFT JOIN PRODUTO_PRECO P1 ON P1.FK_PRODUTO = P.ID AND P1.FK_TABELA_PRECO = 1
LEFT JOIN PRODUTO_PRECO P2 ON P2.FK_PRODUTO = P.ID AND P2.FK_TABELA_PRECO = 2
