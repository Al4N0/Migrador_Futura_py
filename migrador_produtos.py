import fdb
import mysql.connector
from core import ConexaoFirebird, ConexaoMySQL
from loguru import logger

class MigradorProdutos:
    def __init__(self, fb_conn: ConexaoFirebird, my_conn: ConexaoMySQL, id_loja: int, log_callback=None):
        self.fb = fb_conn
        self.my = my_conn
        self.id_loja = id_loja
        self.log = log_callback if log_callback else print
        self._batch_size = 1000

    def executar(self, truncar=False):
        try:
            self.log(f"> Iniciando migração de Produtos (Loja {self.id_loja})...")
            
            sucesso_fb, msg_fb = self.fb.conectar()
            if not sucesso_fb:
                self.log(f"Erro ao conectar no Firebird: {msg_fb}")
                return False

            sucesso_my, msg_my = self.my.conectar()
            if not sucesso_my:
                self.log(f"Erro ao conectar no MySQL: {msg_my}")
                return False

            if truncar:
                self.log("> Limpando a tabela 'produto' no MySQL (TRUNCATE)...")
                cursor_my = self.my.conn.cursor()
                # Disable checks for faster truncate without FK constraints issues
                cursor_my.execute("SET FOREIGN_KEY_CHECKS = 0;")
                cursor_my.execute("TRUNCATE TABLE produto")
                cursor_my.execute("SET FOREIGN_KEY_CHECKS = 1;")
                self.my.conn.commit()
                cursor_my.close()

            self.log("> Extraindo produtos do Firebird... (ISSO PODE DEMORAR)")
            dados = self.extrair_dados()
            
            if not dados:
                self.log("> Nenhum produto retornado ou erro na extração.")
                return False
                
            self.log(f"> Foram encontrados {len(dados)} produtos.")

            self.log("> Salvando produtos no MySQL...")
            self.salvar_no_mysql(dados)

            self.log("✅ Migração de Produtos concluída com sucesso!")
            return True

        except Exception as e:
            self.log(f"❌ Erro na migração de produtos: {e}")
            logger.error(f"MigradorProdutos falhou: {e}")
            return False
        finally:
            self.fb.desconectar()
            self.my.desconectar()

    def extrair_dados(self):
        query = """
        SELECT
             CASE
                  WHEN PCB.CODIGO_BARRA IS NULL OR PCB.CODIGO_BARRA = '' THEN LEFT(P.ID, 20)
                  ELSE LEFT(PCB.CODIGO_BARRA, 20)
             END AS id,
             LEFT(PF.NRO_FABRICANTE, 45) AS codigofornecedor,
             COALESCE(P.CUSTO, 0) AS custo,
             P.DATA_CADASTRO AS datacadastro,
             LEFT(P.DESCRICAO, 120) AS descricao,
             CASE
                WHEN C.CNPJ_CPF IS NOT NULL AND C.CNPJ_CPF <> '' THEN LEFT(REPLACE(REPLACE(REPLACE(C.CNPJ_CPF, '.', ''), '/', ''), '-', ''), 14)
                ELSE CAST(C.ID AS VARCHAR(14))
             END AS idfornecedor,
             LEFT(CF.CLASSIFICACAO, 8) AS ncm,
             P.OBSERVACAO AS obs,
             COALESCE(P1.VALOR, 0) AS preco1,
             COALESCE(P2.VALOR, 0) AS preco3,
             CASE
                WHEN P.STATUS = '0' THEN 'ATIVO'
                ELSE 'DELETED'
            END AS status,
             'P' AS tipo,
             LEFT(PU.SIGLA, 6) AS unidade
        FROM PRODUTO P
        LEFT JOIN PRODUTO_UNIDADE PU ON PU.ID = P.FK_PRODUTO_UNIDADE
        LEFT JOIN CLASSIFICACAO_FISCAL CF ON CF.ID = P.FK_CLASSIFICACAO_FISCAL
        LEFT JOIN PRODUTO_CODIGO_BARRA PCB ON PCB.FK_PRODUTO = P.ID
        LEFT JOIN PRODUTO_FORNECEDOR PF ON PF.FK_PRODUTO = P.ID
        LEFT JOIN CADASTRO C ON C.ID = PF.FK_FORNECEDOR
        LEFT JOIN PRODUTO_PRECO P1 ON P1.FK_PRODUTO = P.ID AND P1.FK_TABELA_PRECO = 1
        LEFT JOIN PRODUTO_PRECO P2 ON P2.FK_PRODUTO = P.ID AND P2.FK_TABELA_PRECO = 2
        """
        
        cursor = self.fb.conn.cursor()
        cursor.execute(query)
        colunas = [desc[0].lower() for desc in cursor.description]
        
        resultados = []
        ids_vistos = set()
        
        for row in cursor.fetchall():
            linha_dict = dict(zip(colunas, row))
            linha_dict['idloja'] = self.id_loja
            
            id_atual = str(linha_dict.get('id', '')).strip()
            if not id_atual:
                continue # Pula se nao conseguir gerar um ID valido
                
            if id_atual in ids_vistos:
                continue # TODO: ou lidar com duplicação se necessário (ignorar por enquanto pois código de barra duplicado já deve estar mitigado no left join)
                
            ids_vistos.add(id_atual)
            resultados.append(linha_dict)
            
        cursor.close()
        return resultados

    def salvar_no_mysql(self, dados):
        if not dados:
            return

        cursor = self.my.conn.cursor()
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute("SET UNIQUE_CHECKS = 0;")
        
        colunas = list(dados[0].keys())
        nomes_colunas = ", ".join([f"`{col}`" for col in colunas])
        placeholders = ", ".join(["%s"] * len(colunas))
        
        sql = f"REPLACE INTO produto ({nomes_colunas}) VALUES ({placeholders})"
        valores = []

        total_inseridos = 0
        lote_atual = []

        try:
            for linha in dados:
                lote_atual.append(tuple(linha[col] for col in colunas))
                
                if len(lote_atual) >= self._batch_size:
                    cursor.executemany(sql, lote_atual)
                    self.my.conn.commit()
                    total_inseridos += len(lote_atual)
                    lote_atual.clear()
                    
            if lote_atual:
                cursor.executemany(sql, lote_atual)
                self.my.conn.commit()
                total_inseridos += len(lote_atual)

            self.log(f"  > Foram enviados e processados {total_inseridos} produtos.")
        except Exception as e:
            self.my.conn.rollback()
            self.log(f"  > Erro ao gravar lote de Produtos no MySQL: {e}")
            raise
        finally:
            cursor.execute("SET UNIQUE_CHECKS = 1;")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            cursor.close()
