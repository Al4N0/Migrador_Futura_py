import fdb
import os

FB_PATH = r'C:\Users\ALANO\Desktop\Migração\0329 - 1 Exinia_Silvateles\0329 - 1 exinia.fdb'
FB_HOST = 'localhost'
FB_PORT = 3050
FB_USER = 'SYSDBA'
FB_PASS = 'masterkey'

def check_order(target_id):
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
        
        print(f"Buscando Pedido ID ou NRO_PEDIDO: {target_id}...")
        cur.execute(f"SELECT ID, NRO_PEDIDO, FK_TIPO_PEDIDO, TOTAL_PEDIDO FROM PEDIDO WHERE ID = {target_id} OR NRO_PEDIDO = {target_id}")
        row = cur.fetchone()
        
        if not row:
            print(f"Pedido {target_id} não encontrado.")
        else:
            db_id, db_nro, db_tipo, db_total = row
            print(f"Encontrado! ID: {db_id}, NRO_PEDIDO: {db_nro}, Tipo: {db_tipo}, Total: {db_total}")
            
            # Verificar itens
            cur.execute(f"SELECT COUNT(*), SUM(QUANTIDADE) FROM PEDIDO_ITEM WHERE FK_PEDIDO = {db_id}")
            count, items_sum = cur.fetchone()
            print(f"Itens em PEDIDO_ITEM para ID {db_id}: Qtd Itens={count}, Soma Quantidade={items_sum}")
            
            # Verificar se tem itens em outra tabela?
            cur.execute(f"SELECT COUNT(*) FROM CAIXA_ITEM WHERE FK_PEDIDO = {db_id}")
            caixa_count = cur.fetchone()[0]
            print(f"Registros em CAIXA_ITEM para ID {db_id}: {caixa_count}")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    import sys
    tid = 10022203
    if len(sys.argv) > 1:
        try:
            tid = int(sys.argv[1])
        except:
            pass
    check_order(tid)
