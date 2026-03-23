import fdb
import mysql.connector
from core import ConexaoFirebird, ConexaoMySQL
from loguru import logger

class MigradorVendedores:
    def __init__(self, fb_conn: ConexaoFirebird, my_conn: ConexaoMySQL, id_loja: int, log_callback=None):
        self.fb = fb_conn
        self.my = my_conn
        self.id_loja = id_loja
        self.log = log_callback if log_callback else print
        self._batch_size = 1000

    def executar(self, truncar=False):
        try:
            self.log(f"> Iniciando migração de Vendedores (Loja {self.id_loja})...")
            
            sucesso_fb, msg_fb = self.fb.conectar()
            if not sucesso_fb:
                self.log(f"Erro ao conectar no Firebird: {msg_fb}")
                return False

            sucesso_my, msg_my = self.my.conectar()
            if not sucesso_my:
                self.log(f"Erro ao conectar no MySQL: {msg_my}")
                return False

            if truncar:
                self.log("> Limpando a tabela 'vendedor' no MySQL (TRUNCATE)...")
                cursor_my = self.my.conn.cursor()
                cursor_my.execute("SET FOREIGN_KEY_CHECKS = 0;")
                cursor_my.execute("TRUNCATE TABLE vendedor")
                cursor_my.execute("SET FOREIGN_KEY_CHECKS = 1;")
                self.my.conn.commit()
                cursor_my.close()

            self.log("> Extraindo vendedores do Firebird...")
            dados = self.extrair_dados()
            
            if not dados:
                self.log("> Nenhum vendedor retornado ou erro na extração.")
                return False
                
            self.log(f"> Foram encontrados {len(dados)} vendedores.")

            self.log("> Salvando vendedores no MySQL...")
            self.salvar_no_mysql(dados)

            self.log("✅ Migração de Vendedores concluída com sucesso!")
            return True

        except Exception as e:
            self.log(f"❌ Erro na migração de vendedores: {e}")
            logger.error(f"MigradorVendedores falhou: {e}")
            return False
        finally:
            self.fb.desconectar()
            self.my.desconectar()

    def extrair_dados(self):
        query = """
        SELECT
            LEFT(C.RAZAO_SOCIAL, 30) AS id,
            '1' AS ativo,
            C.RAZAO_SOCIAL AS nome,
            CE.BAIRRO AS bairro,
            CE.FONE AS fone,
            C.E_MAIL AS email,
            CE.LOGRADOURO AS logradouro,
            CE.CEP AS cep,
            CE.NUMERO AS numero,
            CE.CIDADE AS cidade,
            CE.UF AS uf,
            C.ID AS codigo,
            CE.COMPLEMENTO AS complemento,
            'ATIVO' AS status
        FROM CADASTRO C 
        LEFT JOIN CADASTRO_ENDERECO CE ON CE.FK_CADASTRO = C.ID
        WHERE C.CHK_VENDEDOR = 'S'
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
        
        sql = f"REPLACE INTO vendedor ({nomes_colunas}) VALUES ({placeholders})"
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

            self.log(f"  > Foram enviados e processados {total_inseridos} vendedores.")
        except Exception as e:
            self.my.conn.rollback()
            self.log(f"  > Erro ao gravar lote de Vendedores no MySQL: {e}")
            raise
        finally:
            cursor.execute("SET UNIQUE_CHECKS = 1;")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            cursor.close()
