import fdb
import mysql.connector
from core import ConexaoFirebird, ConexaoMySQL
from loguru import logger

class MigradorFornecedores:
    def __init__(self, fb_conn: ConexaoFirebird, my_conn: ConexaoMySQL, id_loja: int, log_callback=None):
        self.fb = fb_conn
        self.my = my_conn
        self.id_loja = id_loja
        self.log = log_callback if log_callback else print
        self._batch_size = 1000

    def executar(self, truncar=False):
        try:
            self.log(f"> Iniciando migração de Fornecedores (Loja {self.id_loja})...")
            
            sucesso_fb, msg_fb = self.fb.conectar()
            if not sucesso_fb:
                self.log(f"Erro ao conectar no Firebird: {msg_fb}")
                return False

            sucesso_my, msg_my = self.my.conectar()
            if not sucesso_my:
                self.log(f"Erro ao conectar no MySQL: {msg_my}")
                return False

            if truncar:
                self.log("> Limpando a tabela 'fornecedor' no MySQL (TRUNCATE)...")
                cursor_my = self.my.conn.cursor()
                cursor_my.execute("SET FOREIGN_KEY_CHECKS = 0;")
                cursor_my.execute("TRUNCATE TABLE fornecedor")
                cursor_my.execute("SET FOREIGN_KEY_CHECKS = 1;")
                self.my.conn.commit()
                cursor_my.close()

            self.log("> Extraindo fornecedores do Firebird...")
            dados = self.extrair_dados()
            
            if not dados:
                self.log("> Nenhum fornecedor retornado ou erro na extração.")
                return False
                
            self.log(f"> Foram encontrados {len(dados)} fornecedores.")

            self.log("> Salvando fornecedores no MySQL...")
            self.salvar_no_mysql(dados)

            self.log("✅ Migração de Fornecedores concluída com sucesso!")
            return True

        except Exception as e:
            self.log(f"❌ Erro na migração de fornecedores: {e}")
            logger.error(f"MigradorFornecedores falhou: {e}")
            return False
        finally:
            self.fb.desconectar()
            self.my.desconectar()

    def extrair_dados(self):
        query = """
        SELECT
            CASE 
               WHEN c.CNPJ_CPF IS NOT NULL AND TRIM(c.CNPJ_CPF) <> '' THEN LEFT(REPLACE(REPLACE(REPLACE(c.CNPJ_CPF, '.', ''), '-', ''), '/', ''), 18)
               ELSE CAST(c.ID AS CHAR(18))
            END AS id,
            LEFT(c.RAZAO_SOCIAL, 80) AS nome,
            CASE
                WHEN CHAR_LENGTH(C.CNPJ_CPF) = 18 THEN C.CNPJ_CPF
                ELSE NULL
            END AS cnpj,
            CE.LOGRADOURO AS logradouro,
            CE.NUMERO AS numero,
            LEFT(CE.COMPLEMENTO, 20) AS complemento,
            CE.BAIRRO AS bairro,
            UF.MUNICIPIO AS cidade,
            UF.UF AS uf,
            CE.CEP AS cep,
            CASE 
                WHEN c.INSCRICAO_RG = 'INSENTO' THEN NULL
                WHEN CHAR_LENGTH(c.CNPJ_CPF) = 18 THEN LEFT(REPLACE(REPLACE(REPLACE(c.INSCRICAO_RG, '.', ''), '-', ''), '/', ''), 20)
                ELSE NULL
            END AS ie,
            UF.CODIGO_MUNICIPIO AS idcidade,
            CE.FONE AS fone,
            P.CODIGO AS idpais,
            C.E_MAIL AS email,
            C.AVISO AS aviso,
            C.OBSERVACAO AS observacao,
            C.STATUS AS status,
            REPLACE(REPLACE(REPLACE(CE.CELULAR, '(', ''), ')', ''), '-', '') AS celular,
            '1' AS ativo,
            LEFT(C.FANTASIA, 50) AS fantasia,
            C.DATA_CADASTRO AS datacadastro
        FROM CADASTRO C
        LEFT JOIN CADASTRO_ENDERECO CE ON CE.FK_CADASTRO = C.ID
        LEFT JOIN PAISES P ON P.ID = CE.FK_PAISES 
        LEFT JOIN UF_MUNICIPIO_IBGE UF ON CE.FK_UF_MUNICIPIO_IBGE = UF.ID
        WHERE C.CHK_FORNECEDOR = 'S'
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
                continue
                
            if id_atual in ids_vistos:
                continue
                
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
        
        sql = f"REPLACE INTO fornecedor ({nomes_colunas}) VALUES ({placeholders})"
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

            self.log(f"  > Foram enviados e processados {total_inseridos} fornecedores.")
        except Exception as e:
            self.my.conn.rollback()
            self.log(f"  > Erro ao gravar lote de Fornecedores no MySQL: {e}")
            raise
        finally:
            cursor.execute("SET UNIQUE_CHECKS = 1;")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            cursor.close()
