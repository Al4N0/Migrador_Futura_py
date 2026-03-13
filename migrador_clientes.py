import fdb
import mysql.connector
from core import ConexaoFirebird, ConexaoMySQL
from loguru import logger

class MigradorClientes:
    def __init__(self, fb_conn: ConexaoFirebird, my_conn: ConexaoMySQL, log_callback=None):
        self.fb = fb_conn
        self.my = my_conn
        self.log = log_callback if log_callback else print

    def executar(self, truncar=False):
        try:
            self.log("> Iniciando migração de Clientes...")
            
            # 1. Conectar aos bancos
            sucesso_fb, msg_fb = self.fb.conectar()
            if not sucesso_fb:
                self.log(f"Erro ao conectar no Firebird: {msg_fb}")
                return False

            sucesso_my, msg_my = self.my.conectar()
            if not sucesso_my:
                self.log(f"Erro ao conectar no MySQL: {msg_my}")
                return False

            # 2. Criar os índices no Firebird (otimização)
            self.criar_indices_firebird()

            # Opcional: Truncar a tabela destino
            if truncar:
                self.log("> Limpando a tabela 'cliente' no MySQL (TRUNCATE)...")
                cursor_my = self.my.conn.cursor()
                cursor_my.execute("TRUNCATE TABLE cliente")
                self.my.conn.commit()
                cursor_my.close()

            # 3. Extrair dados do Firebird
            self.log("> Extraindo clientes do Firebird... (ISSO PODE DEMORAR)")
            dados = self.extrair_dados()
            
            if not dados:
                self.log("> Nenhum cliente retornado ou erro na extração.")
                return False
                
            self.log(f"> Foram encontrados {len(dados)} clientes.")

            # 4. Inserir no MySQL
            self.log("> Salvando clientes no MySQL...")
            self.salvar_no_mysql(dados)

            self.log("✅ Migração de Clientes concluída com sucesso!")
            return True

        except Exception as e:
            self.log(f"❌ Erro na migração de clientes: {e}")
            logger.error(f"MigradorClientes falhou: {e}")
            return False
        finally:
            self.fb.desconectar()
            self.my.desconectar()

    def criar_indices_firebird(self):
        self.log("> Verificando/Criando índices no Firebird (isso otimiza a query)...")
        indices_sql = [
            "CREATE DESCENDING INDEX IDX_CADEND_FK_ID ON CADASTRO_ENDERECO (FK_CADASTRO, ID);",
            "CREATE INDEX IDX_CAD_FKVEND ON CADASTRO (FK_VENDEDOR);",
            "CREATE INDEX IDX_CADFISC_FKCAD ON CADASTRO_FISCAL (FK_CADASTRO);",
            "CREATE INDEX IDX_CADFISC_FKCNAE ON CADASTRO_FISCAL (FK_CNAE);",
            "CREATE INDEX IDX_CADEND_FKUF ON CADASTRO_ENDERECO (FK_UF_MUNICIPIO_IBGE);",
            "CREATE INDEX IDX_CADEND_FKPAIS ON CADASTRO_ENDERECO (FK_PAISES);"
        ]
        fb_cursor = self.fb.conn.cursor()
        
        for sql in indices_sql:
            try:
                fb_cursor.execute(sql)
                self.fb.conn.commit()
            except fdb.fbcore.DatabaseError as e:
                # O erro -607 geralmente é "unsuccessful metadata update", ex: "index already exists"
                if "already exists" in str(e).lower() or "-607" in str(e):
                    # Ignorar se já existe
                    pass
                else:
                    self.log(f"  Aviso ao criar índice: {e}")
                    
        fb_cursor.close()

    def extrair_dados(self):
        query = """
        SELECT
            CASE 
                WHEN c.CNPJ_CPF IS NOT NULL AND TRIM(c.CNPJ_CPF) <> '' THEN LEFT(REPLACE(REPLACE(REPLACE(c.CNPJ_CPF, '.', ''), '-', ''), '/', ''), 18)
                ELSE CAST(c.ID AS CHAR(18))
            END	AS id,                          -- char(18)
            c3.DESCRICAO AS atividade,                          -- text
            CASE
                WHEN c.STATUS = 1 THEN 1
                ELSE 0
            END AS ativo,                          -- tinyint(1)
            LEFT(c.AVISO, 50) AS aviso,                          -- varchar(255)
            LEFT(ce.BAIRRO, 30) AS bairro,                          -- varchar(30)
            LEFT(ce.CELULAR, 20) AS celular,                           -- char(20)
            ce.CEP AS cep,                          -- char(9)
            LEFT(uf.MUNICIPIO, 50) AS cidade,                        -- varchar(50)
            c.ID AS codigo,                          -- int
            c.ID AS codigomigrado,                          -- int
            LEFT(ce.COMPLEMENTO, 20) AS complemento,                          -- varchar(20)
            LEFT(ce.CONTATO, 20) AS comprador,                          -- char(20)
            c.DATA_CADASTRO AS datacadastro,                          -- date
            LEFT(c.E_MAIL, 50) AS email,                          -- varchar(50)
            LEFT(C.FANTASIA, 30) AS fantasia,                          -- varchar(30)
            LEFT(ce.FONE, 30) AS fone,                          -- char(30)
            uf.CODIGO_MUNICIPIO AS idcidade,                          -- int
            1 AS idloja,                          -- smallint
            p.CODIGO AS idpais,                          -- smallint
            'C' AS idtipo,                          -- char(1)
            LEFT(c2.RAZAO_SOCIAL, 30) AS idvendedor,                          -- char(30)
            CASE 
                WHEN c.INSCRICAO_RG = 'INSENTO' THEN NULL
                WHEN CHAR_LENGTH(c.CNPJ_CPF) = 18 THEN LEFT(REPLACE(REPLACE(REPLACE(c.INSCRICAO_RG, '.', ''), '-', ''), '/', ''), 20)
                ELSE NULL
            END AS ie,                          -- char(20)
            LEFT(ce.LOGRADOURO, 80) AS logradouro,                           -- varchar(80)
            '1' AS lojaorigem,                          -- smallint
            c.OUTRAS_OBSERVACOES AS memo2,                          -- text
            LEFT(c.RAZAO_SOCIAL, 80) AS nome,                          -- char(80)
            ce.NUMERO AS numero,                           -- varchar(20)
            c.OBSERVACAO AS observacao,                          -- text
            CASE
                WHEN c.FISICA_JURIDICA = 'J' THEN 1
                ELSE 0     	
            END AS pessoajuridica,                          -- tinyint(1)
            CASE 
                WHEN CHAR_LENGTH(c.CNPJ_CPF) < 18 AND c.INSCRICAO_RG <> '' THEN LEFT(c.INSCRICAO_RG, 20)
                ELSE NULL
            END AS rg,                          -- char(18)     
            LEFT(c.SITE, 100) AS site,                          -- varchar(100)
            CASE
                WHEN c.STATUS = 1 THEN 'ATIVA'
                ELSE 'INATIVA'
            END AS status,                          -- char(10)
            CASE
                WHEN c.INSCRICAO_SUFRAMA IS NOT NULL AND c.INSCRICAO_SUFRAMA <> '' THEN LEFT(c.INSCRICAO_SUFRAMA, 20)
                ELSE NULL
            END AS suframa,                          -- char(20)
            c.FISICA_JURIDICA AS tipo,                          -- char(1)
            ce.UF AS uf,                          -- char(2)     
            CASE 
                WHEN CHAR_LENGTH(c.CNPJ_CPF) < 18 THEN LEFT(c.CNPJ_CPF, 18)
            END AS cpf,                          -- char(18)     
            LEFT(REPLACE(REPLACE(REPLACE(REPLACE(ce.WHATSAPP, '(', ''), ')', ''), '-', ''), ' ', ''), 11) AS whatsapp,                      -- char(11)
            c.DATA_NASCIMENTO AS aniversario 
        FROM CADASTRO c
        LEFT JOIN CADASTRO c2 ON c.FK_VENDEDOR = c2.ID 
        LEFT JOIN CADASTRO_ENDERECO ce ON ce.ID = (
            SELECT MAX(CE_INT.ID)
            FROM CADASTRO_ENDERECO CE_INT
            WHERE CE_INT.FK_CADASTRO = c.ID)
        LEFT JOIN UF_MUNICIPIO_IBGE uf ON ce.FK_UF_MUNICIPIO_IBGE = uf.ID 	
        LEFT JOIN PAISES p ON ce.FK_PAISES = p.ID 
        LEFT JOIN CADASTRO_FISCAL cf ON c.ID = cf.FK_CADASTRO
        LEFT JOIN CNAE c3 ON cf.FK_CNAE = c3.ID
        WHERE c.CHK_CLIENTE = 'S'
        """
        
        cursor = self.fb.conn.cursor()
        cursor.execute(query)
        # O FDB retorna uma lista de tuplas. Vamos transformar num formato mais fácil de inserir,
        # pegando o nome das colunas do cursor.description
        colunas = [desc[0].lower() for desc in cursor.description]
        
        resultados = []
        ids_vistos = set()
        
        for row in cursor.fetchall():
            linha_dict = dict(zip(colunas, row))
            
            # 1. Garantia contra ID completamente vazio ou só espaços
            id_atual = str(linha_dict['id']).strip() if linha_dict['id'] else ""
            if not id_atual:
                id_atual = str(linha_dict['codigo']).strip()
                
            linha_dict['id'] = id_atual
                
            # 2. Garantir que não há IDs duplicados (CPF/CNPJ iguais em cadastros diferentes)
            # Se já existir, a regra é forçar o ID do cadastro do Firebird.
            # Se mesmo assim o código colidir com outro CPF/CNPJ, adicionamos um sufixo
            id_original = linha_dict['id']
            base_cod = str(linha_dict['codigo'])
            
            if linha_dict['id'] in ids_vistos:
                linha_dict['id'] = base_cod
                contador = 1
                while linha_dict['id'] in ids_vistos:
                    linha_dict['id'] = f"{base_cod}-{contador}"
                    contador += 1
                
            # 3. Registrar o ID que será usado de fato nesta linha
            ids_vistos.add(linha_dict['id'])
            
            resultados.append(linha_dict)
            
        cursor.close()
        return resultados

    def salvar_no_mysql(self, dados):
        if not dados:
            return

        cursor = self.my.conn.cursor()
        
        # O REPLACE INTO funciona como um UPSERT se houver Primary Key idêntica.
        # Caso o cliente já exista, ele sobrescreve com os dados novos.
        
        colunas = list(dados[0].keys())
        nomes_colunas = ", ".join([f"`{col}`" for col in colunas])
        placeholders = ", ".join(["%s"] * len(colunas))
        
        sql = f"REPLACE INTO cliente ({nomes_colunas}) VALUES ({placeholders})"
        
        # Prepara a lista de valores (lista de tuplas)
        valores = []
        for linha in dados:
            # Transformando de dicionário para tupla na mesma ordem das colunas
            valores.append(tuple(linha[col] for col in colunas))

        # mysql.connector executemany lida bem com lotes grandes (ex: thousands of rows)
        try:
            cursor.executemany(sql, valores)
            self.my.conn.commit()
            
            # O REPLACE INTO no MySQL conta 1 linha para Insert novo, 
            # e 2 linhas para um Update (ele apaga a anterior e insere a nova).
            # Para não confundir, usamos o tamanho da lista que enviamos.
            self.log(f"  > Foram enviados e processados {len(dados)} clientes.")
            self.log(f"  > (Linhas afetadas internamente pelo MySQL: {cursor.rowcount})")
        except Exception as e:
            self.my.conn.rollback()
            self.log(f"  > Erro ao gravar lote no MySQL: {e}")
            raise
        finally:
            cursor.close()
