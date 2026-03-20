import fdb
from core import ConexaoFirebird, ConexaoMySQL
from loguru import logger

class MigradorParcelas:
    """
    Migra os registros (Parcelas financeiras) baseados na query mista (CONTA_PARCELA + CAIXA_ITEM) 
    do Firebird para a tabela `parcela` do MySQL.
    
    Assim como MigradorItens, utiliza o pattern de pré-carregamento para acelerar a migração.
    """

    # Colunas da tabela MySQL (na ordem do INSERT)
    _COLUNAS_MYSQL = [
        "idloja", "idvenda", "parcela", "idplano", "idforma", "taxid", "idcliente",
        "idusuario", "data", "documento", "pixcopiacola", "prazo",
        "vencimento", "valor", "valorfinal", "valorrecebido", "created", "quitado"
    ]

    def __init__(
        self,
        fb_conn: ConexaoFirebird,
        my_conn: ConexaoMySQL,
        id_loja: int,
        fk_empresa: int,
        mapa_idvenda: dict,
        mapping_pagamento: dict,
        log_callback=None,
    ):
        self.fb = fb_conn
        self.my = my_conn
        self.id_loja = id_loja
        self.fk_empresa = fk_empresa
        self.mapa_idvenda = mapa_idvenda
        self.mapping_pagamento = mapping_pagamento
        self.log = log_callback if log_callback else print

        # Estatísticas
        self.total_inseridos = 0
        self.total_erros = 0

        # Cache em memória: { id_pedido_fb: [dict, dict, ...] }
        self._cache: dict[int, list[dict]] = {}

        nomes = ", ".join([f"`{c}`" for c in self._COLUNAS_MYSQL])
        placeholders = ", ".join(["%s"] * len(self._COLUNAS_MYSQL))
        self._sql_insert = f"INSERT INTO parcela ({nomes}) VALUES ({placeholders})"

    def pre_carregar(self, cursor_fb) -> int:
        self.log(f"> Pré-carregando parcelas do Firebird (empresa {self.fk_empresa})...")

        query = f"""
        SELECT * FROM (
            -- 1. USA PARCELAS (somente quando fecha o pedido)
            SELECT
                P.ID AS fk_pedido,
                ROW_NUMBER() OVER (PARTITION BY P.ID ORDER BY CP.DATA_VENCIMENTO, CP.ID) AS parcela,
                P.ID AS taxid,
                CASE
                    WHEN C2.CNPJ_CPF IS NOT NULL AND C2.CNPJ_CPF <> '' THEN LEFT(REPLACE(REPLACE(REPLACE(C2.CNPJ_CPF, '.', ''), '/', ''), '-', ''), 14)
                    ELSE CAST(C2.ID AS VARCHAR(14))
                END AS idcliente,
                C.FK_USUARIO AS idusuario,
                P.DATA_EMISSAO AS data,
                CP.DOCUMENTO AS documento,
                CA.DESCRICAO AS pixcopiacola,
                COALESCE(DATEDIFF(DAY FROM P.DATA_EMISSAO TO CP.DATA_VENCIMENTO), 0) AS prazo,
                CP.DATA_VENCIMENTO AS vencimento,
                CP.VALOR_PARCELA AS valor,
                CP.VALOR_PARCELA AS valorfinal,
                0 AS valorrecebido,
                P.DATA_EMISSAO AS created,
                CASE WHEN CP.VALOR_PAGO > 0 THEN 1 ELSE 0 END AS quitado,
                CASE WHEN C.FK_CARTAO IS NOT NULL AND C.FK_CARTAO > 0 THEN CA.DESCRICAO ELSE '' END AS forma_pagamento,
                (
                    SELECT COUNT(CP2.ID) 
                    FROM CONTA_PARCELA CP2 
                    WHERE CP2.FK_CONTA = C.ID
                ) AS total_parcelas
            FROM PEDIDO P
            JOIN CONTA C ON C.FK_PEDIDO = P.ID    
            JOIN CONTA_PARCELA CP ON CP.FK_CONTA = C.ID
            LEFT JOIN CADASTRO C2 ON C2.ID = P.FK_CADASTRO
            LEFT JOIN CARTAO CA ON CA.ID = C.FK_CARTAO
            WHERE P.STATUS NOT IN ('1','3') 
            AND (P.FK_TIPO_PEDIDO = '1' OR P.FK_TIPO_PEDIDO = '5')
            AND P.FK_EMPRESA = {self.fk_empresa}
            AND (
                SELECT COALESCE(SUM(CP2.VALOR_PARCELA), 0)
                FROM CONTA_PARCELA CP2
                JOIN CONTA C2 ON C2.ID = CP2.FK_CONTA
                WHERE C2.FK_PEDIDO = P.ID
            ) = P.TOTAL_PEDIDO

            UNION ALL

            -- 2. USA CAIXA (somente quando NÃO tem parcela suficiente)
            SELECT
                P.ID AS fk_pedido,
                ROW_NUMBER() OVER (PARTITION BY P.ID ORDER BY CI.DATA_HORA, CI.ID) AS parcela,
                P.ID AS taxid,
                CASE
                    WHEN C2.CNPJ_CPF IS NOT NULL AND C2.CNPJ_CPF <> '' THEN LEFT(REPLACE(REPLACE(REPLACE(C2.CNPJ_CPF, '.', ''), '/', ''), '-', ''), 14)
                    ELSE CAST(C2.ID AS VARCHAR(14))
                END AS idcliente,
                CI.FK_USUARIO AS idusuario,
                P.DATA_EMISSAO AS data,
                NULL AS documento,
                CA.DESCRICAO AS pixcopiacola,
                COALESCE(DATEDIFF(DAY FROM P.DATA_EMISSAO TO CI.DATA_HORA), 0) AS prazo,
                P.DATA_EMISSAO AS vencimento,
                CI.VALOR AS valor,
                CI.VALOR AS valorfinal,
                0 AS valorrecebido,
                P.DATA_EMISSAO AS created,
                1 AS quitado,
                CASE WHEN CI.FK_CARTAO IS NOT NULL AND CI.FK_CARTAO > 0 THEN CA.DESCRICAO ELSE TP.DESCRICAO END AS forma_pagamento,
                (
                    SELECT COUNT(CI2.ID) 
                    FROM CAIXA_ITEM CI2 
                    WHERE CI2.FK_PEDIDO = P.ID
                ) AS total_parcelas
            FROM PEDIDO P
            JOIN CAIXA_ITEM CI ON CI.FK_PEDIDO = P.ID
            LEFT JOIN CADASTRO C2 ON C2.ID = P.FK_CADASTRO    
            LEFT JOIN CARTAO CA ON CA.ID = CI.FK_CARTAO
            LEFT JOIN TIPO_PAGAMENTO TP ON TP.ID = CI.FK_TIPO_PAGAMENTO
            WHERE P.STATUS NOT IN ('1','3')
            AND (P.FK_TIPO_PEDIDO = '1' OR P.FK_TIPO_PEDIDO = '5')
            AND P.FK_EMPRESA = {self.fk_empresa}
            AND NOT EXISTS (
                SELECT 1
                FROM CONTA C
                JOIN CONTA_PARCELA CP ON CP.FK_CONTA = C.ID
                WHERE C.FK_PEDIDO = P.ID
                GROUP BY C.FK_PEDIDO
                HAVING SUM(CP.VALOR_PARCELA) = P.TOTAL_PEDIDO
            )
        ) X
        """

        cursor_fb.execute(query)
        colunas = [desc[0].lower() for desc in cursor_fb.description]
        
        self._cache.clear()
        total = 0
        for row in cursor_fb.fetchall():
            linha = dict(zip(colunas, row))
            # O fk_pedido é retirado pois não vai persistir como coluna, mas a referência é necessária
            fk_ped = linha.pop("fk_pedido")
            
            # Aplicar o mapeamento de pagamento
            forma = linha.get("forma_pagamento", "")
            if forma is None: forma = ""
            total_parc = linha.get("total_parcelas", 0)
            if total_parc is None: total_parc = 0
            
            forma_x = f"{forma} {total_parc}X".strip() if total_parc > 0 else forma
            
            if forma_x in self.mapping_pagamento:
                idplano_mapped = self.mapping_pagamento[forma_x]
            else:
                idplano_mapped = self.mapping_pagamento.get(forma)
                
            if idplano_mapped is not None:
                linha["idplano"] = idplano_mapped
                linha["idforma"] = idplano_mapped
            else:
                linha["idplano"] = None
                linha["idforma"] = None

            self._cache.setdefault(fk_ped, []).append(linha)
            total += 1

        self.log(f"  > {total:,} parcelas carregadas em memória para {len(self._cache):,} pedidos.")
        return total

    def inserir_por_venda(self, id_pedido_fb: int, cursor_my) -> bool:
        """
        Lê do cache e insere as parcelas da venda no MySQL.
        """
        idvenda_mysql = self.mapa_idvenda.get(id_pedido_fb)
        if idvenda_mysql is None:
            return False

        parcelas = self._cache.get(id_pedido_fb, [])
        if not parcelas:
            return True

        lote = [self._montar_valores(p, idvenda_mysql) for p in parcelas]

        try:
            cursor_my.executemany(self._sql_insert, lote)
            self.total_inseridos += len(lote)
            return True
        except Exception as e:
            self.total_erros += len(lote)
            self.log(f"  ⚠️ Erro ao inserir {len(lote)} parcelas do pedido FB {id_pedido_fb}: {e}")
            logger.error(f"Erro ao inserir parcelas pedido {id_pedido_fb}: {e}")
            return False

    def _montar_valores(self, parcela: dict, idvenda_mysql: int) -> tuple:
        valores = []
        for col in self._COLUNAS_MYSQL:
            if col == "idloja":
                valores.append(self.id_loja)
            elif col == "idvenda":
                valores.append(idvenda_mysql)
            else:
                valores.append(parcela.get(col))
        return tuple(valores)
