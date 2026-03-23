import fdb
import mysql.connector
from core import ConexaoFirebird, ConexaoMySQL
from loguru import logger

class MigradorUsuarios:
    def __init__(self, fb_conn: ConexaoFirebird, my_conn: ConexaoMySQL, id_loja: int, log_callback=None):
        self.fb = fb_conn
        self.my = my_conn
        self.id_loja = id_loja
        self.log = log_callback if log_callback else print
        self._batch_size = 1000

    def executar(self, truncar=False):
        try:
            self.log(f"> Iniciando migração de Usuarios (Loja {self.id_loja})...")
            
            sucesso_fb, msg_fb = self.fb.conectar()
            if not sucesso_fb:
                self.log(f"Erro ao conectar no Firebird: {msg_fb}")
                return False

            sucesso_my, msg_my = self.my.conectar()
            if not sucesso_my:
                self.log(f"Erro ao conectar no MySQL: {msg_my}")
                return False

            if truncar:
                self.log("> Limpando a tabela 'usuario' no MySQL (TRUNCATE)...")
                cursor_my = self.my.conn.cursor()
                cursor_my.execute("SET FOREIGN_KEY_CHECKS = 0;")
                cursor_my.execute("TRUNCATE TABLE usuario")
                cursor_my.execute("SET FOREIGN_KEY_CHECKS = 1;")
                self.my.conn.commit()
                cursor_my.close()

            self.log("> Extraindo usuarios do Firebird...")
            dados = self.extrair_dados()
            
            if not dados:
                self.log("> Nenhum usuario retornado ou erro na extração.")
                return False
                
            self.log(f"> Foram encontrados {len(dados)} usuarios.")

            self.log("> Salvando usuarios no MySQL...")
            self.salvar_no_mysql(dados)

            self.log("✅ Migração de Usuarios concluída com sucesso!")
            return True

        except Exception as e:
            self.log(f"❌ Erro na migração de usuarios: {e}")
            logger.error(f"MigradorUsuarios falhou: {e}")
            return False
        finally:
            self.fb.desconectar()
            self.my.desconectar()

    def extrair_dados(self):
        query = """
        SELECT 	 
            U.NOME AS login,
            U.NOME AS nome,
            '1234' AS senha,
            '1' AS preferencial,
            '1' AS nivel,
            'CAIXA' AS grupo,     
            C.E_MAIL AS email,
            (TRIM(U.NOME) || ' ' || TRIM(U.NOME) || ' CAIXA') AS tag,
            CASE 
                WHEN U.STATUS = 0 THEN 'ATIVO'
                ELSE 'INATIVO'
            END AS status,
            CASE 
                WHEN U.STATUS = 0 THEN 1
                ELSE 0
            END AS ATIVO	
        FROM SYS_USUARIO U
        LEFT JOIN CADASTRO C ON C.ID = U.FK_FUNCIONARIO
        """
        
        cursor = self.fb.conn.cursor()
        cursor.execute(query)
        colunas = [desc[0].lower() for desc in cursor.description]
        
        resultados = []
        
        for row in cursor.fetchall():
            linha_dict = dict(zip(colunas, row))
            linha_dict['idloja'] = self.id_loja
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
        
        # Treatment for AES_ENCRYPT on 'senha' column
        placeholders_list = []
        for col in colunas:
            if col == 'senha':
                placeholders_list.append("AES_ENCRYPT(%s, 'miredata')")
            else:
                placeholders_list.append("%s")
        
        placeholders = ", ".join(placeholders_list)
        
        sql = f"REPLACE INTO usuario ({nomes_colunas}) VALUES ({placeholders})"
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

            self.log(f"  > Foram enviados e processados {total_inseridos} usuarios.")
        except Exception as e:
            self.my.conn.rollback()
            self.log(f"  > Erro ao gravar lote de Usuarios no MySQL: {e}")
            raise
        finally:
            cursor.execute("SET UNIQUE_CHECKS = 1;")
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
            cursor.close()
