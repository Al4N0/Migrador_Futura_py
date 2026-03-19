import fdb
import os
from dotenv import load_dotenv

load_dotenv()

FB_PATH = os.getenv('FB_PATH')
FB_HOST = os.getenv('FB_HOST', 'localhost')
FB_PORT = int(os.getenv('FB_PORT', 3050))
FB_USER = os.getenv('FB_USER', 'SYSDBA')
FB_PASS = os.getenv('FB_PASS', 'masterkey')

def check_returns(target_id=10022203):
    try:
        conn = fdb.connect(
            host=FB_HOST,
            database=FB_PATH,
            user=FB_USER,
            password=FB_PASS,
            port=FB_PORT,
            charset='WIN1252'
        )
        cur = conn.cursor()
        
        print(f"Checking order ID: {target_id}...")
        query = f"""
        SELECT
            V.ID, V.FK_TIPO_PEDIDO, V.TOTAL_PEDIDO,
            (SELECT SUM(QUANTIDADE) FROM PEDIDO_ITEM WHERE FK_PEDIDO = V.ID) as item_sum
        FROM PEDIDO V
        WHERE V.ID = {target_id}
        """
        cur.execute(query)
        row = cur.fetchone()
        if not row:
            print(f"Order ID {target_id} not found in PEDIDO table.")
            # Search by NRO_PEDIDO just in case
            print(f"Searching by NRO_PEDIDO = {target_id}...")
            cur.execute(f"SELECT ID, FK_TIPO_PEDIDO, TOTAL_PEDIDO FROM PEDIDO WHERE NRO_PEDIDO = {target_id}")
            row = cur.fetchone()
            if row:
                print(f"Found by NRO_PEDIDO! ID: {row[0]}, Tip: {row[1]}, Total: {row[2]}")
                # Check items for THIS id
                cur.execute(f"SELECT SUM(QUANTIDADE) FROM PEDIDO_ITEM WHERE FK_PEDIDO = {row[0]}")
                item_sum = cur.fetchone()[0]
                print(f"Item sum for internal ID {row[0]}: {item_sum}")
        else:
            print(f"ID: {row[0]}, Tip: {row[1]}, Total: {row[2]}, Item Sum: {row[3]}")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    import sys
    tid = 10022203
    if len(sys.argv) > 1:
        tid = int(sys.argv[1])
    check_returns(tid)
