import fdb
from core import ConexaoFirebird, ConexaoMySQL
from loguru import logger


class MigradorItens:
    """
    Migra os registros de PEDIDO_ITEM (Firebird) para a tabela `item` (MySQL).

    Performance: usa pré-carregamento.
      - pre_carregar(cursor_fb, fk_empresa): executa UMA query no Firebird que
        traz TODOS os itens da empresa, agrupa em memória por FK_PEDIDO.
      - inserir_por_venda(): lê do cache em memória (sem round-trip ao Firebird)
        e usa executemany() para inserir todos os itens de uma venda de uma vez.

    → Elimina 100 mil queries individuais ao Firebird.
    → Reduz round-trips MySQL de N por venda para 1 por venda (executemany).
    """

    # Colunas da tabela `item` no MySQL (na ordem do INSERT)
    _COLUNAS_MYSQL = [
        "idloja", "idvenda", "cancelado", "desconto", "iditem",
        "idestoque", "log", "oper", "preco", "porcento",
        "quant", "referencia", "status", "total",
        "idvtex", "idtuddu", "created", "updated",
    ]

    def __init__(
        self,
        fb_conn: ConexaoFirebird,
        my_conn: ConexaoMySQL,
        id_loja: int,
        fk_empresa: int,
        mapa_idvenda: dict,
        log_callback=None,
    ):
        self.fb = fb_conn
        self.my = my_conn
        self.id_loja = id_loja
        self.fk_empresa = fk_empresa
        self.mapa_idvenda = mapa_idvenda
        self.log = log_callback if log_callback else print

        # Estatísticas
        self.total_inseridos = 0
        self.total_erros = 0

        # Cache pré-carregado: { id_pedido_fb: [dict, dict, ...] }
        self._cache: dict[int, list[dict]] = {}

        # SQL de inserção pré-montado
        nomes = ", ".join([f"`{c}`" for c in self._COLUNAS_MYSQL])
        placeholders = ", ".join(["%s"] * len(self._COLUNAS_MYSQL))
        self._sql_insert = f"INSERT INTO item ({nomes}) VALUES ({placeholders})"

    # =========================================================================
    # PRÉ-CARREGAMENTO: UMA query para todos os itens
    # =========================================================================

    def pre_carregar(self, cursor_fb) -> int:
        """
        Executa UMA query no Firebird trazendo todos os itens da empresa.
        Agrupa em self._cache por FK_PEDIDO.
        Retorna o total de itens carregados.

        Deve ser chamado ANTES do loop principal de vendas.
        """
        self.log(f"> Pré-carregando itens do Firebird (empresa {self.fk_empresa})...")

        query = f"""
        SELECT
            I.FK_PEDIDO,
            CASE
                WHEN V.STATUS = 3 THEN 1
                ELSE 0
            END AS cancelado,
            COALESCE(I.VALOR_DESCONTO, 0) AS desconto,
            I.SEQUENCIA AS iditem,
            CASE
                WHEN PCB.CODIGO_BARRA IS NULL OR PCB.CODIGO_BARRA = ''
                    THEN LEFT(CAST(P.ID AS VARCHAR(30)), 30)
                ELSE LEFT(PCB.CODIGO_BARRA, 30)
            END AS idestoque,
            0 AS log,
            CASE
                WHEN V.FK_TIPO_PEDIDO = 1 THEN 'V'
                WHEN V.FK_TIPO_PEDIDO = 5 THEN 'D'
            END AS oper,
            COALESCE(I.VALOR_UNITARIO, 0) AS preco,
            CASE
                WHEN (I.QUANTIDADE * I.VALOR_UNITARIO) > 0
                    THEN (COALESCE(I.VALOR_DESCONTO, 0) / (I.QUANTIDADE * I.VALOR_UNITARIO)) * 100
                ELSE 0
            END AS porcento,
            COALESCE(I.QUANTIDADE, 0) AS quant,
            CASE
                WHEN PCB.CODIGO_BARRA IS NULL OR PCB.CODIGO_BARRA = ''
                    THEN LEFT(CAST(P.ID AS VARCHAR(20)), 20)
                ELSE LEFT(PCB.CODIGO_BARRA, 20)
            END AS referencia,
            CASE
                WHEN V.STATUS = 1 THEN 'E'
                WHEN V.STATUS = 3 THEN 'C'
                WHEN V.STATUS = 2 THEN 'F'
                WHEN V.STATUS = 4 THEN 'N'
            END AS status,
            (I.QUANTIDADE * COALESCE(I.VALOR_UNITARIO, 0)) AS total,
            V.ID AS idvtex,
            CAST(V.NRO_PEDIDO AS VARCHAR(30)) AS idtuddu,
            I.DATA_HORA AS created,
            I.DATA_HORA AS updated
        FROM PEDIDO_ITEM I
        LEFT JOIN PEDIDO V ON V.ID = I.FK_PEDIDO
        LEFT JOIN PRODUTO P ON P.ID = I.FK_PRODUTO
        LEFT JOIN PRODUTO_CODIGO_BARRA PCB ON PCB.FK_PRODUTO = P.ID
        WHERE (V.FK_TIPO_PEDIDO = '1' OR V.FK_TIPO_PEDIDO = '5')
          AND V.FK_EMPRESA = {self.fk_empresa}
        """

        cursor_fb.execute(query)
        colunas = [desc[0].lower() for desc in cursor_fb.description]

        self._cache.clear()
        total = 0
        for row in cursor_fb.fetchall():
            linha = dict(zip(colunas, row))
            fk_ped = linha.pop("fk_pedido")  # remove chave auxiliar, guarda como chave do dict
            self._cache.setdefault(fk_ped, []).append(linha)
            total += 1

        self.log(f"  > {total:,} itens carregados em memória para {len(self._cache):,} pedidos.")
        return total

    # =========================================================================
    # INSERÇÃO: usa cache em memória + executemany
    # =========================================================================

    def inserir_por_venda(
        self,
        id_pedido_fb: int,
        cursor_fb,          # mantido por compatibilidade, não usado se cache ativo
        cursor_my,
    ) -> bool:
        """
        Insere todos os itens do pedido `id_pedido_fb` no MySQL.
        Usa cache pré-carregado se disponível (modo rápido).
        Usa executemany() para inserir todos os itens de uma vez.
        """
        idvenda_mysql = self.mapa_idvenda.get(id_pedido_fb)
        if idvenda_mysql is None:
            return False

        # ── Busca itens (cache ou Firebird) ───────────────────────────
        if self._cache:
            itens = self._cache.get(id_pedido_fb, [])
        else:
            itens = self._extrair_itens_firebird(id_pedido_fb, cursor_fb)

        if not itens:
            return True  # Pedido sem itens — ok

        # ── Monta lista de tuplas para executemany ────────────────────
        lote = [self._montar_valores(item, idvenda_mysql) for item in itens]

        try:
            cursor_my.executemany(self._sql_insert, lote)
            self.total_inseridos += len(lote)
            return True
        except Exception as e:
            self.total_erros += len(lote)
            self.log(
                f"  ⚠️ Erro ao inserir {len(lote)} itens do pedido FB {id_pedido_fb}: {e}"
            )
            logger.error(f"Erro ao inserir itens pedido {id_pedido_fb}: {e}")
            return False

    # =========================================================================
    # FALLBACK: extração por pedido (sem cache)
    # =========================================================================

    def _extrair_itens_firebird(self, id_pedido_fb: int, cursor_fb) -> list[dict]:
        """Fallback: busca itens de um único pedido no Firebird."""
        query = """
        SELECT
            CASE WHEN V.STATUS = 3 THEN 1 ELSE 0 END AS cancelado,
            COALESCE(I.VALOR_DESCONTO, 0) AS desconto,
            I.SEQUENCIA AS iditem,
            CASE
                WHEN PCB.CODIGO_BARRA IS NULL OR PCB.CODIGO_BARRA = ''
                    THEN LEFT(CAST(P.ID AS VARCHAR(30)), 30)
                ELSE LEFT(PCB.CODIGO_BARRA, 30)
            END AS idestoque,
            0 AS log,
            CASE WHEN V.FK_TIPO_PEDIDO = 1 THEN 'V' WHEN V.FK_TIPO_PEDIDO = 5 THEN 'D' END AS oper,
            COALESCE(I.VALOR_UNITARIO, 0) AS preco,
            CASE
                WHEN (I.QUANTIDADE * I.VALOR_UNITARIO) > 0
                    THEN (COALESCE(I.VALOR_DESCONTO, 0) / (I.QUANTIDADE * I.VALOR_UNITARIO)) * 100
                ELSE 0
            END AS porcento,
            COALESCE(I.QUANTIDADE, 0) AS quant,
            CASE
                WHEN PCB.CODIGO_BARRA IS NULL OR PCB.CODIGO_BARRA = ''
                    THEN LEFT(CAST(P.ID AS VARCHAR(20)), 20)
                ELSE LEFT(PCB.CODIGO_BARRA, 20)
            END AS referencia,
            CASE
                WHEN V.STATUS = 1 THEN 'E' WHEN V.STATUS = 3 THEN 'C'
                WHEN V.STATUS = 2 THEN 'F' WHEN V.STATUS = 4 THEN 'N'
            END AS status,
            (I.QUANTIDADE * COALESCE(I.VALOR_UNITARIO, 0)) AS total,
            V.ID AS idvtex,
            CAST(V.NRO_PEDIDO AS VARCHAR(30)) AS idtuddu,
            I.DATA_HORA AS created,
            I.DATA_HORA AS updated
        FROM PEDIDO_ITEM I
        LEFT JOIN PEDIDO V ON V.ID = I.FK_PEDIDO
        LEFT JOIN PRODUTO P ON P.ID = I.FK_PRODUTO
        LEFT JOIN PRODUTO_CODIGO_BARRA PCB ON PCB.FK_PRODUTO = P.ID
        WHERE I.FK_PEDIDO = ?
        """
        cursor_fb.execute(query, (id_pedido_fb,))
        colunas = [desc[0].lower() for desc in cursor_fb.description]
        return [dict(zip(colunas, row)) for row in cursor_fb.fetchall()]

    # =========================================================================
    # MONTAGEM DE VALORES
    # =========================================================================

    def _montar_valores(self, item: dict, idvenda_mysql: int) -> tuple:
        valores = []
        for col in self._COLUNAS_MYSQL:
            if col == "idloja":
                valores.append(self.id_loja)
            elif col == "idvenda":
                valores.append(idvenda_mysql)
            else:
                valores.append(item.get(col))
        return tuple(valores)
