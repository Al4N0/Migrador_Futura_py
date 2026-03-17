import fdb
import mysql.connector
from loguru import logger

class ConexaoFirebird:
    def __init__(self, path, user, password, host="localhost", port=3050):
        self.path = path
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.conn = None

    def conectar(self):
        try:
            # No Windows, o Firebird (via fdb) espera o caminho do arquivo codificado em ANSI (cp1252).
            # Se passarmos a string direto, o fdb usa utf-8 e quebra acentos ("MigraÃ§Ã£o" em vez de "Migração").
            dsn_path = self.path
            
            # Montar DSN sempre no formato host/port:path (TCP/IP)
            # Necessário quando o Firebird roda em Docker, mesmo em localhost
            dsn_string = f"{self.host}/{self.port}:{dsn_path}"
                
            dsn_encoded = dsn_string.encode('cp1252') if isinstance(dsn_string, str) else dsn_string
            
            self.conn = fdb.connect(
                dsn=dsn_encoded,
                user=self.user,
                password=self.password,
                charset='WIN1252' # Padrão comum no Firebird no Windows
            )
            return True, "Conectado ao Firebird com sucesso!"
        except Exception as e:
            logger.error(f"Erro Firebird: {str(e)}")
            return False, f"Erro FB: {str(e)}"

    def desconectar(self):
        if self.conn:
            self.conn.close()

class ConexaoMySQL:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None

    def conectar(self):
        try:
            self.conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.conn.is_connected():
                return True, "Conectado ao MySQL com sucesso!"
            return False, "Falha desconhecida ao conectar no MySQL."
        except Exception as e:
            logger.error(f"Erro MySQL: {str(e)}")
            return False, f"Erro MySQL: {str(e)}"

    def desconectar(self):
        if self.conn and self.conn.is_connected():
            self.conn.close()
