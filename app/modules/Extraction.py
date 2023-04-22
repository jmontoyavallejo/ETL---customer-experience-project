import pandas as pd
import psycopg2
import requests
import io
import pandas_gbq


def full_extraction():
    db_host = "35.239.114.35"
    db_name = "test-data-mining"
    db_user = "test-data-mining"
    db_password = "test-data-mining-lax-2022"

    conn = psycopg2.connect(
        host=db_host,
        dbname=db_name,
        user=db_user,
        password=db_password
    )

    cur  = conn.cursor()

    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

    # Obtener los resultados y mostrar las tablas disponibles
    tables = cur.fetchall()
    print("Tablas disponibles:")
    for table in tables:
        print(table[0])

    tabla = "mineria_almacen"
    cur.execute(f"SELECT * FROM {tabla}")
    resultados_mineria_almacen = cur.fetchall()
    columnas = [desc[0] for desc in cur.description]
    df_mineria_almacen = pd.DataFrame(resultados_mineria_almacen, columns=columnas)

    tabla = "mineria_producto"
    cur.execute(f"SELECT * FROM {tabla}")
    resultados_mineria_producto = cur.fetchall()
    columnas = [desc[0] for desc in cur.description]
    df_mineria_producto = pd.DataFrame(resultados_mineria_producto, columns=columnas)

    # tabla = "mineria_venta"
    # cur.execute(f"SELECT * FROM {tabla}")
    # resultados_mineria_venta = cur.fetchall()
    # columnas = [desc[0] for desc in cur.description]
    # df_mineria_venta = pd.DataFrame(resultados_mineria_venta, columns=columnas)

    # tabla = "mineria_resultado_campanas"
    # cur.execute(f"SELECT * FROM {tabla}")
    # resultados_mineria_resultado_campanas = cur.fetchall()
    # columnas = [desc[0] for desc in cur.description]
    # df_mineria_resultado_campanas = pd.DataFrame(resultados_mineria_resultado_campanas, columns=columnas)

    cur.close()
    conn.close()
    url = "https://v1-api.lax.marketing/api/blacklist/downloadFile"

    payload={}
    headers = {
    'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZFVzZXIiOjg5LCJpZENvbXBhbnkiOjQzLCJyb2xVc2VyIjoyLCJpYXQiOjE2ODIwODM2NDN9.HvKEkVpeSmC3xnfq9HFzpT2usSewnTfBDkOVNMQ2k9Q'
    }
    response = requests.request("GET", url, headers=headers, data=payload)
    df_blacklist = pd.read_csv(io.StringIO(response.content.decode('utf-8')),sep=';')
    
    df_mineria_ventas_csv=pd.read_csv('mineria_venta.csv',sep=';')
    df_mineria_resultado_campanas_csv=pd.read_csv('mineria_resultado_campanas.csv',sep=';')

    table_id_blacklist = 'customer-experience-384423.Data_cruda.blacklist'
    table_id_mineria_ventas_csv = 'customer-experience-384423.Data_cruda.mineria_venta'
    table_id_mineria_resultado_campanas_csv = 'customer-experience-384423.Data_cruda.mineria_resultado_campanas'
    table_id_mineria_producto = 'customer-experience-384423.Data_cruda.mineria_producto'
    table_id_mineria_almacen= 'customer-experience-384423.Data_cruda.mineria_almacen'

    pandas_gbq.to_gbq(df_blacklist.astype('string'), table_id_blacklist, project_id='customer-experience-384423', if_exists='replace')
    pandas_gbq.to_gbq(df_mineria_ventas_csv.astype('string'), table_id_mineria_ventas_csv, project_id='customer-experience-384423', if_exists='replace')
    pandas_gbq.to_gbq(df_mineria_resultado_campanas_csv.astype('string'), table_id_mineria_resultado_campanas_csv, project_id='customer-experience-384423', if_exists='replace')
    pandas_gbq.to_gbq(df_mineria_producto.astype('string'), table_id_mineria_producto, project_id='customer-experience-384423', if_exists='replace')
    pandas_gbq.to_gbq(df_mineria_almacen.astype('string'), table_id_mineria_almacen, project_id='customer-experience-384423', if_exists='replace')