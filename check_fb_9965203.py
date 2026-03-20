import fdb
import os
from dotenv import load_dotenv

load_dotenv()
try:
    path_fb = os.getenv("FB_PATH")
    user_fb = os.getenv("FB_USER", "SYSDBA")
    pass_fb = os.getenv("FB_PASS", "masterkey")
    host_fb = os.getenv("FB_HOST", "localhost")
    port_fb = int(os.getenv("FB_PORT", "3050"))

    conn = fdb.connect(dsn=f"{host_fb}/{port_fb}:{path_fb}", user=user_fb, password=pass_fb)
    cur = conn.cursor()

    print("PEDIDO ID 10022403")
    cur.execute("SELECT ID, TOTAL_PEDIDO FROM PEDIDO WHERE ID = 10022403")
    print("PEDIDO:", cur.fetchall())

    cur.execute('''
        SELECT C.ID, C.FK_CARTAO, CA.DESCRICAO, TP.DESCRICAO AS TIPO
        FROM CONTA C 
        LEFT JOIN CARTAO CA ON CA.ID = C.FK_CARTAO
        LEFT JOIN PEDIDO P ON P.ID = C.FK_PEDIDO
        LEFT JOIN TIPO_PAGAMENTO TP ON TP.ID = P.FK_TIPO_PEDIDO
        WHERE C.FK_PEDIDO = 10022403
    ''')
    print("CONTA:", cur.fetchall())

    cur.execute('''
        SELECT CI.ID, CI.FK_CARTAO, CA.DESCRICAO, CI.FK_TIPO_PAGAMENTO, TP.DESCRICAO AS TIPO, CI.VALOR
        FROM CAIXA_ITEM CI
        LEFT JOIN CARTAO CA ON CA.ID = CI.FK_CARTAO
        LEFT JOIN TIPO_PAGAMENTO TP ON TP.ID = CI.FK_TIPO_PAGAMENTO
        WHERE CI.FK_PEDIDO = 10022403
    ''')
    print("CAIXA_ITEM:", cur.fetchall())

    conn.close()
except Exception as e:
    print(f"Erro: {e}")
