import fdb
from core import ConexaoFirebird, ConexaoMySQL
from loguru import logger


class MigradorVendas:
    """
    Migra os registros de PEDIDO (Firebird) para a tabela `venda` (MySQL).

    Parâmetros obrigatórios na construção:
      - fb_conn     : ConexaoFirebird
      - my_conn     : ConexaoMySQL
      - id_loja     : int  → valor do campo idloja na tabela destino
      - fk_empresa  : int  → filtro V.FK_EMPRESA no Firebird (selecionado pelo usuário)
      - log_callback: callable opcional para exibir mensagens na UI

    Resultado após executar():
      - self.mapa_idvenda: dict { id_pedido_firebird: idvenda_mysql }
        Esse dicionário será usado nas etapas seguintes (itens e parcelas).
    """

    def __init__(
        self,
        fb_conn: ConexaoFirebird,
        my_conn: ConexaoMySQL,
        id_loja: int,
        fk_empresa: int,
        log_callback=None,
    ):
        self.fb = fb_conn
        self.my = my_conn
        self.id_loja = id_loja
        self.fk_empresa = fk_empresa
        self.log = log_callback if log_callback else print
        # Dicionário id_pedido_fb → idvenda_mysql – preenchido após executar()
        self.mapa_idvenda: dict[int, int] = {}

    # =========================================================================
    # PONTO DE ENTRADA PÚBLICO
    # =========================================================================

    def executar(self, truncar: bool = False) -> bool:
        try:
            self.log("> Iniciando migração de Vendas...")

            # 1. Conectar
            sucesso_fb, msg_fb = self.fb.conectar()
            if not sucesso_fb:
                self.log(f"❌ Erro ao conectar no Firebird: {msg_fb}")
                return False

            sucesso_my, msg_my = self.my.conectar()
            if not sucesso_my:
                self.log(f"❌ Erro ao conectar no MySQL: {msg_my}")
                return False

            # 2. Opcional: truncar tabela destino
            if truncar:
                self.log("> Limpando a tabela 'venda' no MySQL (TRUNCATE)...")
                cur = self.my.conn.cursor()
                cur.execute("SET FOREIGN_KEY_CHECKS = 0")
                cur.execute("TRUNCATE TABLE venda")
                cur.execute("SET FOREIGN_KEY_CHECKS = 1")
                self.my.conn.commit()
                cur.close()

            # 3. Criar índices no Firebird (otimização)
            self._criar_indices_firebird()

            # 4. Extrair dados do Firebird
            self.log(f"> Extraindo vendas do Firebird (empresa {self.fk_empresa})...")
            dados = self._extrair_dados()
            if not dados:
                self.log("> Nenhuma venda retornada ou erro na extração.")
                return False

            self.log(f"> Foram encontrados {len(dados)} pedidos.")

            # 5. Inserir no MySQL capturando idvenda gerado
            self.log("> Salvando vendas no MySQL...")
            self._salvar_no_mysql(dados)

            self.log(f"✅ Migração de Vendas concluída! {len(self.mapa_idvenda)} registros mapeados.")
            return True

        except Exception as e:
            self.log(f"❌ Erro na migração de vendas: {e}")
            logger.error(f"MigradorVendas falhou: {e}")
            return False
        finally:
            self.fb.desconectar()
            self.my.desconectar()

    # =========================================================================
    # ÍNDICES FIREBIRD
    # =========================================================================

    def _criar_indices_firebird(self):
        self.log("> Verificando/Criando índices no Firebird...")
        indices_sql = [
            "CREATE INDEX IDX_PEDIDO_EMPRESA_TIPO ON PEDIDO (FK_EMPRESA, FK_TIPO_PEDIDO);",
            "CREATE INDEX IDX_PEDIDO_TIPOPED ON PEDIDO (FK_TIPO_PEDIDO);",
            "CREATE INDEX IDX_PEDIDO_EMPRESA  ON PEDIDO (FK_EMPRESA);",
            "CREATE INDEX IDX_PEDIDO_CADASTRO ON PEDIDO (FK_CADASTRO);",
            "CREATE INDEX IDX_PEDIDO_VENDEDOR  ON PEDIDO (FK_VENDEDOR);",
            "CREATE INDEX IDX_PEDIDO_USUARIO  ON PEDIDO (FK_USUARIO_PED);",
            "CREATE INDEX IDX_CXITEM_PEDIDO   ON CAIXA_ITEM (FK_PEDIDO);",
            "CREATE INDEX IDX_PITEM_PEDIDO    ON PEDIDO_ITEM (FK_PEDIDO);",
            "CREATE INDEX IDX_NF_PEDIDO       ON NOTA_FISCAL (FK_PEDIDO);",
            "CREATE INDEX IDX_CONTA_PEDIDO    ON CONTA (FK_PEDIDO);",
            "CREATE INDEX IDX_CPARCELA_CONTA  ON CONTA_PARCELA (FK_CONTA);",
        ]
        cur = self.fb.conn.cursor()
        for sql in indices_sql:
            try:
                cur.execute(sql)
                self.fb.conn.commit()
            except fdb.fbcore.DatabaseError as e:
                err_str = str(e).lower()
                if "already exists" in err_str or "-607" in str(e):
                    pass  # índice já existe – ok
                else:
                    self.log(f"  Aviso ao criar índice: {e}")
        cur.close()

    # =========================================================================
    # EXTRAÇÃO FIREBIRD
    # =========================================================================

    def _extrair_dados(self) -> list[dict]:
        """
        Executa o SELECT de vendas no Firebird.
        Retorna lista de dicionários, cada chave corresponde a uma coluna
        (EXCETO idvenda/idloja que serão gerados/injetados no MySQL).

        Coluna extra incluída:
          _id_pedido_fb  →  V.ID original do Firebird (usado para o mapa).
        """
        query = f"""
        SELECT
            'V'                                                    AS operacao,
            ROW_NUMBER() OVER(PARTITION BY V.DATA_EMISSAO
                              ORDER BY V.DATA_HORA_ABERTURA)       AS romaneio,
            CASE
                WHEN SU.STATUS = 1 THEN SU.ID
            END                                                    AS idusuario,
            CASE
                WHEN CCLI.CNPJ_CPF IS NOT NULL AND CCLI.CNPJ_CPF <> ''
                    THEN LEFT(
                            REPLACE(REPLACE(REPLACE(CCLI.CNPJ_CPF, '.', ''), '/', ''), '-', ''),
                            14)
                ELSE CAST(CCLI.ID AS VARCHAR(14))
            END                                                    AS idcliente,
            V.DATA_EMISSAO                                         AS datacadastro,
            T.QUANTIDADE                                           AS quantidade,
            V.TOTAL_PRODUTO                                        AS total,
            V.TOTAL_PEDIDO                                         AS liquido,
            CASE
                WHEN FIN.FK_PEDIDO IS NOT NULL AND FIN.FK_PEDIDO > 0
                    THEN COALESCE(FIN.TOTAL_PARCELAS, 0)
                ELSE CI.TOTAL_PARCELAS
            END                                                    AS parcela,
            CASE
                WHEN FIN.FK_PEDIDO IS NOT NULL AND FIN.FK_PEDIDO > 0
                    THEN DATEDIFF(DAY FROM V.DATA_EMISSAO TO FIN.VENCIMENTO)
                ELSE DATEDIFF(DAY FROM V.DATA_EMISSAO TO CI.DATA_VENCIMENTO)
            END                                                    AS prazo,
            V.NRO_PEDIDO                                           AS pedido,
            CASE
                WHEN V.STATUS = 1 THEN 'E'
                WHEN V.STATUS = 3 THEN 'C'
                WHEN V.STATUS = 2 THEN 'F'
                WHEN V.STATUS = 4 THEN 'N'
            END                                                    AS status,
            CI.IDPLANO                                             AS idplano,
            LEFT(CVEND.RAZAO_SOCIAL, 30)                          AS idvendedor,
            LEFT(V.OBSERVACAO, 100)                               AS obs,
            V.MOTIVO_CANCELAMENTO                                  AS motivo,
            CASE
                WHEN FIN.FK_PEDIDO IS NOT NULL AND FIN.FK_PEDIDO > 0
                     AND FIN.VENCIMENTO <= (CURRENT_DATE - 1)     THEN 1
                WHEN CI.DATA_VENCIMENTO <= (CURRENT_DATE - 1)     THEN 1
                ELSE 0
            END                                                    AS quitado,
            CASE
                WHEN V.STATUS = 1 THEN 'N'
                ELSE 'G'
            END                                                    AS aberto,
            V.DATA_HORA_FECHAMENTO                                 AS data,
            V.DATA_HORA_FECHAMENTO                                 AS hora,
            CASE
                WHEN V.FK_TABELA_PRECO = 201 THEN 1
                ELSE 0
            END                                                    AS online,
            CASE
                WHEN V.FK_TABELA_PRECO = 2 OR V.FK_TABELA_PRECO = 101 THEN 1
                ELSE 0
            END                                                    AS varejo,
            V.FK_USUARIO_PED_CANCELAMENTO                         AS idautorizacao,
            V.FK_ULTIMO_USUARIO_EDICAO                            AS idusuarioalteracao,
            V.ID                                                    AS orderidvtex,
            NF.NRO_NOTA                                            AS nnf,
            CASE
                WHEN CI.FK_CARTAO IS NOT NULL AND CI.FK_CARTAO > 0
                    THEN C.DESCRICAO
                ELSE TP.DESCRICAO
            END                                                    AS idpacking,
            V.DATA_HORA_ABERTURA                                   AS created,
            V.DATA_HORA_ABERTURA                                   AS updated,
            V.ID                                                    AS id_pedido_fb
        FROM PEDIDO V
            LEFT JOIN CADASTRO CCLI  ON CCLI.ID  = V.FK_CADASTRO
            LEFT JOIN CADASTRO CVEND ON CVEND.ID = V.FK_VENDEDOR
            LEFT JOIN NOTA_FISCAL NF ON NF.FK_PEDIDO = V.ID
            LEFT JOIN (
                SELECT FK_PEDIDO, SUM(QUANTIDADE) AS QUANTIDADE
                FROM PEDIDO_ITEM
                GROUP BY FK_PEDIDO
            ) T  ON T.FK_PEDIDO = V.ID
            LEFT JOIN (
                SELECT
                    FK_PEDIDO,
                    MIN(FK_TIPO_PAGAMENTO) AS IDPLANO,
                    MIN(FK_CARTAO)         AS FK_CARTAO,
                    COUNT(FK_PEDIDO)       AS TOTAL_PARCELAS,
                    MAX(DATA_HORA)         AS DATA_VENCIMENTO
                FROM CAIXA_ITEM
                GROUP BY FK_PEDIDO
            ) CI ON CI.FK_PEDIDO = V.ID
            LEFT JOIN (
                SELECT
                    C_INT.FK_PEDIDO,
                    MAX(CP_INT.DATA_VENCIMENTO) AS VENCIMENTO,
                    COUNT(CP_INT.ID)            AS TOTAL_PARCELAS
                FROM CONTA C_INT
                JOIN CONTA_PARCELA CP_INT ON CP_INT.FK_CONTA = C_INT.ID
                GROUP BY C_INT.FK_PEDIDO
            ) FIN ON FIN.FK_PEDIDO = V.ID
            LEFT JOIN SYS_USUARIO SU   ON SU.ID  = V.FK_USUARIO_PED
            LEFT JOIN TIPO_PAGAMENTO TP ON TP.ID = CI.IDPLANO
            LEFT JOIN CARTAO C          ON C.ID  = CI.FK_CARTAO
        WHERE V.FK_TIPO_PEDIDO = '1'
          AND V.FK_EMPRESA = {self.fk_empresa}
        """

        cur = self.fb.conn.cursor()
        cur.execute(query)
        colunas = [desc[0].lower() for desc in cur.description]

        resultados = []
        for row in cur.fetchall():
            linha = dict(zip(colunas, row))
            resultados.append(linha)

        cur.close()
        return resultados

    # =========================================================================
    # INSERÇÃO MYSQL
    # =========================================================================

    def _salvar_no_mysql(self, dados: list[dict]):
        """
        Insere cada venda individualmente para capturar o LAST_INSERT_ID()
        gerado pelo autoincremento do MySQL.
        Preenche self.mapa_idvenda { id_pedido_fb: idvenda_mysql }.
        """
        cursor = self.my.conn.cursor()

        # Colunas que vão para o MySQL (excluir campo auxiliar)
        # Colunas do MySQL (exclui id_pedido_fb que é auxiliar)
        colunas_mysql = [
            "idloja", "operacao", "romaneio", "idusuario", "idcliente",
            "datacadastro", "quantidade", "total", "liquido", "parcela",
            "prazo", "pedido", "status", "idplano", "idvendedor", "obs",
            "motivo", "quitado", "aberto", "data", "hora", "online",
            "varejo", "idautorizacao", "idusuarioalteracao", "orderidvtex",
            "nnf", "idpacking", "created", "updated",
        ]

        nomes = ", ".join([f"`{c}`" for c in colunas_mysql])
        placeholders = ", ".join(["%s"] * len(colunas_mysql))
        sql_insert = f"INSERT INTO venda ({nomes}) VALUES ({placeholders})"

        erros = 0
        for linha in dados:
            id_fb = linha["id_pedido_fb"]

            valores = self._montar_valores(linha, colunas_mysql)

            try:
                cursor.execute(sql_insert, valores)
                idvenda_mysql = cursor.lastrowid
                self.mapa_idvenda[id_fb] = idvenda_mysql
            except Exception as e:
                erros += 1
                self.log(f"  ⚠️ Erro ao inserir pedido FB {id_fb}: {e}")
                logger.error(f"Erro ao inserir pedido {id_fb}: {e}")

        try:
            self.my.conn.commit()
        except Exception as e:
            self.my.conn.rollback()
            self.log(f"  ❌ Erro no commit das vendas: {e}")
            raise
        finally:
            cursor.close()

        self.log(f"  > Inseridos: {len(self.mapa_idvenda)} | Erros: {erros}")

    def _montar_valores(self, linha: dict, colunas: list) -> tuple:
        """
        Monta a tupla de valores na ordem das colunas MySQL.
        Injeta idloja (não vem do Firebird).
        """
        valores = []
        for col in colunas:
            if col == "idloja":
                valores.append(self.id_loja)
            else:
                valores.append(linha.get(col))
        return tuple(valores)
