import fdb
import mysql.connector
from loguru import logger

class ConexaoFirebird:
    def __init__(self, path, user, password):
        self.path = path
        self.user = user
        self.password = password
        self.conn = None

    def conectar(self):
        try:
            self.conn = fdb.connect(
                dsn=self.path,
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
