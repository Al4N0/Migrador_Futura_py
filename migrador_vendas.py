import fdb
from core import ConexaoFirebird, ConexaoMySQL
from migrador_itens import MigradorItens
from migrador_parcelas import MigradorParcelas
from loguru import logger
import json
import os


class MigradorVendas:
    """
    Migra os registros de PEDIDO (Firebird) para as tabelas `venda` e `item` (MySQL).

    Abordagem 1-a-1:
      Para cada pedido Firebird:
        1. INSERT INTO venda  → captura lastrowid → mapa_idvenda[id_fb] = idvenda_mysql
        2. INSERT INTO item   → todos os itens daquele pedido, usando o idvenda recém-gerado
        3. COMMIT             → venda + seus itens são confirmados juntos (atomicidade)

    Parâmetros obrigatórios na construção:
      - fb_conn     : ConexaoFirebird
      - my_conn     : ConexaoMySQL
      - id_loja     : int  → valor do campo idloja na tabela destino
      - fk_empresa  : int  → filtro V.FK_EMPRESA no Firebird (selecionado pelo usuário)
      - log_callback: callable opcional para exibir mensagens na UI

    Resultado após executar():
      - self.mapa_idvenda: dict { id_pedido_firebird: idvenda_mysql }
    """

    def __init__(
        self,
        fb_conn: ConexaoFirebird,
        my_conn: ConexaoMySQL,
        id_loja: int,
        fk_empresa: int,
        log_callback=None,
        progress_callback=None,
    ):
        self.fb = fb_conn
        self.my = my_conn
        self.id_loja = id_loja
        self.fk_empresa = fk_empresa
        self.log = log_callback if log_callback else print
        # progress_callback(atual: int, total: int) → atualiza barra na UI
        self.progress = progress_callback if progress_callback else lambda a, t: None
        # Dicionário id_pedido_fb → idvenda_mysql – preenchido após executar()
        self.mapa_idvenda: dict[int, int] = {}
        # Mapeamento de planos (carregado em executar)
        self._mapping_pagamento: dict[str, int] = {}

    # =========================================================================
    # PONTO DE ENTRADA PÚBLICO
    # =========================================================================

    def executar(self, truncar: bool = False) -> bool:
        try:
            self.log("> Iniciando migração de Vendas + Itens...")

            # 1. Conectar Firebird
            sucesso_fb, msg_fb = self.fb.conectar()
            if not sucesso_fb:
                self.log(f"❌ Erro ao conectar no Firebird: {msg_fb}")
                return False

            # 2. Conectar MySQL
            sucesso_my, msg_my = self.my.conectar()
            if not sucesso_my:
                self.log(f"❌ Erro ao conectar no MySQL: {msg_my}")
                return False

            # 3. Carregar mapeamento de pagamento (JSON)
            self._carregar_mapeamento_pagamento()

            # 3. Opcional: truncar tabelas destino
            if truncar:
                self._truncar_tabelas()

            # 4. Criar índices no Firebird (otimização)
            self._criar_indices_firebird()

            # 5. Extrair cabeçalhos das vendas do Firebird
            self.log(f"> Extraindo vendas do Firebird (empresa {self.fk_empresa})...")
            dados = self._extrair_dados()
            if not dados:
                self.log("> Nenhuma venda retornada ou erro na extração.")
                return False

            total = len(dados)
            self.log(f"> Foram encontrados {total:,} pedidos.")
            # Sinaliza o total para a UI iniciar modo determinado
            self.progress(0, total)

            # 6. Instanciar MigradorItens e MigradorParcelas
            migrador_itens = MigradorItens(
                fb_conn=self.fb,
                my_conn=self.my,
                id_loja=self.id_loja,
                fk_empresa=self.fk_empresa,
                mapa_idvenda=self.mapa_idvenda,
                log_callback=self.log,
            )
            migrador_parcelas = MigradorParcelas(
                fb_conn=self.fb,
                my_conn=self.my,
                id_loja=self.id_loja,
                fk_empresa=self.fk_empresa,
                mapa_idvenda=self.mapa_idvenda,
                mapping_pagamento=self._mapping_pagamento,
                log_callback=self.log,
            )

            # 6b. Pré-carregar TODOS os itens e parcelas em memória
            cur_pre = self.fb.conn.cursor()
            try:
                migrador_itens.pre_carregar(cur_pre)
                migrador_parcelas.pre_carregar(cur_pre)
            finally:
                cur_pre.close()



            # 6d. (Removido: Mapeamento agora é aplicado dinamicamente no loop _salvar_1a1)

            # 7. Loop principal 1-a-1: venda → itens (do cache) → parcelas → commit em lote
            self.log("> Salvando vendas, itens e parcelas no MySQL (1 a 1)...")
            self._salvar_1a1(dados, migrador_itens, migrador_parcelas, total)


            self.log(
                f"✅ Migração concluída! "
                f"Vendas: {len(self.mapa_idvenda)} | "
                f"Itens: {migrador_itens.total_inseridos} ({migrador_itens.total_erros} erros) | "
                f"Parcelas: {migrador_parcelas.total_inseridos} ({migrador_parcelas.total_erros} erros)"
            )
            return True

        except Exception as e:
            self.log(f"❌ Erro na migração de vendas: {e}")
            logger.error(f"MigradorVendas falhou: {e}")
            return False
        finally:
            self.fb.desconectar()
            self.my.desconectar()

    # =========================================================================
    # LOOP 1-A-1
    # =========================================================================

    # Quantidade de vendas por lote de commit (ajuste conforme RAM/latência).
    # Maior = menos round-trips, mais dados em risco se cair no meio.
    BATCH_SIZE = 500

    def _salvar_1a1(self, dados: list[dict], migrador_itens: MigradorItens, migrador_parcelas: MigradorParcelas, total: int):
        """
        Para cada pedido em `dados`:
          1. INSERT na tabela venda → captura lastrowid → atualiza mapa_idvenda
          2. Chama migrador_itens.inserir_por_venda() com cursores compartilhados
          3. COMMIT em lote a cada BATCH_SIZE vendas (muito mais rápido que 1 por 1)

        Em caso de erro por venda, o pedido com erro é pulado (sem rollback do lote
        inteiro), e o lote continua normalmente.
        """
        # Colunas da tabela venda no MySQL
        colunas_venda = [
            "idloja", "operacao", "romaneio", "idusuario", "idcliente",
            "datacadastro", "quantidade", "devolucao", "total", "liquido", "parcela",
            "prazo", "pedido", "status", "idplano", "idvendedor", "obs",
            "motivo", "quitado", "aberto", "data", "hora", "online",
            "varejo", "idautorizacao", "idusuarioalteracao", "orderidvtex",
            "nnf", "idpacking", "created", "updated",
        ]
        nomes = ", ".join([f"`{c}`" for c in colunas_venda])
        placeholders = ", ".join(["%s"] * len(colunas_venda))
        sql_venda = f"INSERT INTO venda ({nomes}) VALUES ({placeholders})"

        cursor_fb = self.fb.conn.cursor()
        cursor_my = self.my.conn.cursor()

        erros_venda = 0

        # ── Otimizações de sessão MySQL ──────────────────────────────
        # Desabilitar verificações desnecessárias durante bulk insert.
        # São restauradas no bloco finally.
        try:
            cursor_my.execute("SET unique_checks = 0")
            cursor_my.execute("SET foreign_key_checks = 0")
            self.log("  > Otimizações de sessão MySQL ativadas.")
        except Exception as e:
            self.log(f"  ⚠️ Não foi possível otimizar sessão MySQL: {e}")

        try:
            for idx, linha in enumerate(dados, start=1):
                id_fb = linha["id_pedido_fb"]

                # Ajusta idplano da Venda processando suas parcelas ativamente
                parcelas_venda = migrador_parcelas._cache.get(id_fb, [])
                formas_distintas = set(p.get("forma_pagamento", "").strip() for p in parcelas_venda if p.get("forma_pagamento"))
                
                if len(formas_distintas) > 1:
                    forma_venda = "MULTIPLAS FORMAS"
                elif len(formas_distintas) == 1:
                    forma_base = list(formas_distintas)[0]
                    total_parcelas = len(parcelas_venda)
                    forma_venda = f"{forma_base} {total_parcelas}X" if total_parcelas > 1 else forma_base
                else:
                    forma_venda = ""

                if forma_venda in self._mapping_pagamento:
                    map_val = self._mapping_pagamento[forma_venda]
                    linha["idplano"] = map_val.get("idplano") if isinstance(map_val, dict) else map_val
                elif len(formas_distintas) == 1 and list(formas_distintas)[0] in self._mapping_pagamento:
                    map_val = self._mapping_pagamento[list(formas_distintas)[0]]
                    linha["idplano"] = map_val.get("idplano") if isinstance(map_val, dict) else map_val


                valores_venda = self._montar_valores_venda(linha, colunas_venda)

                try:
                    # ── INSERT venda ──────────────────────────────────
                    cursor_my.execute(sql_venda, valores_venda)
                    idvenda_mysql = cursor_my.lastrowid
                    self.mapa_idvenda[id_fb] = idvenda_mysql

                    # ── INSERT itens (mesmo cursor, mesma transação) ──
                    migrador_itens.inserir_por_venda(id_fb, cursor_fb, cursor_my)

                    # ── INSERT parcelas (do cache) ──
                    migrador_parcelas.inserir_por_venda(id_fb, cursor_my)

                except Exception as e:
                    erros_venda += 1
                    self.log(f"  ⚠️ Erro ao processar pedido FB {id_fb}: {e}")
                    logger.error(f"Erro no pedido {id_fb}: {e}")

                # ── COMMIT em lote ────────────────────────────────────
                if idx % self.BATCH_SIZE == 0 or idx == total:
                    try:
                        self.my.conn.commit()
                    except Exception as e:
                        self.log(f"  ❌ Erro no commit do lote (vendas {idx - self.BATCH_SIZE + 1}–{idx}): {e}")
                        logger.error(f"Erro no commit do lote: {e}")
                        self.my.conn.rollback()

                    self.log(f"  > [{idx:,}/{total:,}] Vendas processadas (lote commitado)...")
                    self.progress(idx, total)


        finally:
            # Restaurar configurações de sessão MySQL
            try:
                cursor_my.execute("SET unique_checks = 1")
                cursor_my.execute("SET foreign_key_checks = 1")
                self.my.conn.commit()
            except Exception:
                pass
            cursor_fb.close()
            cursor_my.close()

        self.log(
            f"  > Vendas inseridas: {len(self.mapa_idvenda)} | "
            f"Erros de venda: {erros_venda}"
        )

    # =========================================================================
    # TRUNCATE
    # =========================================================================

    def _truncar_tabelas(self):
        """Trunca parcela, item antes de venda (FK), depois venda."""
        self.log("> Limpando tabelas 'parcela', 'item' e 'venda' no MySQL (TRUNCATE)...")
        cur = self.my.conn.cursor()
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS = 0")
            cur.execute("TRUNCATE TABLE parcela")
            cur.execute("TRUNCATE TABLE item")
            cur.execute("TRUNCATE TABLE venda")
            cur.execute("SET FOREIGN_KEY_CHECKS = 1")
            self.my.conn.commit()
            self.log("  > Tabelas truncadas com sucesso.")
        except Exception as e:
            self.log(f"  ⚠️ Erro no truncate: {e}")
            raise
        finally:
            cur.close()

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
            # Usado no JOIN da query de itens: PRODUTO_CODIGO_BARRA.FK_PRODUTO
            "CREATE INDEX IDX_PCB_PRODUTO     ON PRODUTO_CODIGO_BARRA (FK_PRODUTO);",
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
    # EXTRAÇÃO FIREBIRD – CABEÇALHOS DE VENDA
    # =========================================================================

    def _extrair_dados(self) -> list[dict]:
        """
        Executa o SELECT de cabeçalhos de venda no Firebird.
        Retorna lista de dicionários com as colunas do SELECT + id_pedido_fb.
        """
        query = f"""
        SELECT
            CASE
                WHEN V.FK_TIPO_PEDIDO = '1' THEN 'V'
                WHEN V.FK_TIPO_PEDIDO = '5' THEN 'D'
            END AS operacao,
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
            CASE
                WHEN V.FK_TIPO_PEDIDO = '1' THEN T.QUANTIDADE 
                ELSE 0
            END                                          AS quantidade,
            CASE
                WHEN V.FK_TIPO_PEDIDO = '5' THEN T.QUANTIDADE 
                ELSE 0
            END                                          AS devolucao,
            CASE
                WHEN V.FK_TIPO_PEDIDO = '1' THEN V.TOTAL_PRODUTO
                WHEN V.FK_TIPO_PEDIDO = '5' THEN -V.TOTAL_PRODUTO             
            END                                                 AS total,
            CASE
                WHEN V.FK_TIPO_PEDIDO = '1' THEN V.TOTAL_PEDIDO
                WHEN V.FK_TIPO_PEDIDO = '5' THEN -V.TOTAL_PEDIDO               
            END                                                 AS liquido,
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
            LEFT JOIN (
                -- Agrupado para evitar duplicatas quando há > 1 NF por pedido
                SELECT FK_PEDIDO, MAX(NRO_NOTA) AS NRO_NOTA
                FROM NOTA_FISCAL
                GROUP BY FK_PEDIDO
            ) NF ON NF.FK_PEDIDO = V.ID
            LEFT JOIN (
                SELECT FK_PEDIDO, SUM(COALESCE(QUANTIDADE, 0)) AS QUANTIDADE
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
        WHERE (V.FK_TIPO_PEDIDO = '1' OR V.FK_TIPO_PEDIDO = '5')
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
    # MONTAGEM DE VALORES – VENDA
    # =========================================================================

    def _montar_valores_venda(self, linha: dict, colunas: list) -> tuple:
        """
        Monta a tupla de valores na ordem das colunas MySQL da tabela venda.
        Injeta idloja (não vem do Firebird).
        """
        valores = []
        for col in colunas:
            if col == "idloja":
                valores.append(self.id_loja)
            else:
                valores.append(linha.get(col))
        return tuple(valores)

    # =========================================================================
    # MAPEAMENTO DE PAGAMENTO
    # =========================================================================

    def _carregar_mapeamento_pagamento(self):
        """Carrega o JSON de mapeamento, se existir."""
        caminho = "mapping_pagamento.json"
        if os.path.exists(caminho):
            try:
                with open(caminho, "r", encoding="utf-8") as f:
                    self._mapping_pagamento = json.load(f)
                self.log(f"> Mapeamento de formas de pagamento carregado ({len(self._mapping_pagamento)} itens).")
            except Exception as e:
                self.log(f"⚠️ Erro ao carregar mapeamento_pagamento.json: {e}")
        else:
            self.log("> Nenhum mapeamento de formas de pagamento encontrado (usará IDs originais).")

    def _aplicar_mapeamento_pagamento(self, dados: list[dict]):
        """Substitui o idplano nos dados extraídos conforme o mapeamento."""
        if not self._mapping_pagamento:
            return

        count = 0
        for linha in dados:
            forma_origem = linha.get("idpacking") # Campo que contém a descrição/nome na origem
            if forma_origem is None: forma_origem = ""
            parcelas = linha.get("parcela", 0)
            if parcelas is None: parcelas = 0

            # Para lidar com mapeamentos tipo "CREDITO 4X", tenta combinar a forma_origem com " NX"
            forma_x = f"{forma_origem} {parcelas}X".strip() if parcelas > 0 else forma_origem

            if forma_x in self._mapping_pagamento:
                linha["idplano"] = self._mapping_pagamento[forma_x]
                count += 1
            elif forma_origem in self._mapping_pagamento:
                linha["idplano"] = self._mapping_pagamento[forma_origem]
                count += 1
        
        if count > 0:
            self.log(f"  > Mapeamento aplicado em {count:,} vendas.")
