import pandas as pd
import psycopg2
import requests
import io
import pandas_gbq
from google.cloud import storage


class Postgresql_fecth_excecutor():
    '''Class for managing postgresql conections'''

    def __init__(self):
        self.db_host = "35.239.114.35"
        self.db_name = "test-data-mining"
        self.db_user = "test-data-mining"
        self.db_password = "test-data-mining-lax-2022"
        self.conn=self.try_conection()
        self.cur=self.conn.cursor()
    
    def try_conection(self)->None:
        try:
            conn = psycopg2.connect(
                    host=self.db_host,
                    dbname=self.db_name,
                    user=self.db_user,
                    password=self.db_password
                )
            return conn
        except ConnectionError as e:
            raise 'Failed to conect to server'
    
    def download_table(self,table_name)->pd.DataFrame:
        self.cur.execute(f"SELECT * FROM {table_name}")
        results = self.cur.fetchall()
        columns = [desc[0] for desc in self.cur.description]
        df = pd.DataFrame(results, columns=columns)
        df=df.replace("�", "Ñ", regex=True)
        return df
    
    def close_conection(self)->None:
        self.cur.close()
        self.conn.close()

def download_csv_from_gcs(bucket_name, file_name,project_name)->pd.DataFrame:
    """
    Downloads a CSV file from Google Cloud Storage and returns it as a DataFrame.

    Args:
        bucket_name (str): Name of the Google Cloud Storage bucket.
        file_name (str): Name of the CSV file to be downloaded.
        project_name (str): Name of the project in google cloud.

    Returns:
        pd.DataFrame: DataFrame containing the data from the downloaded CSV file.
    """
    storage_client = storage.Client(project=project_name)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    file_string = blob.download_as_text()
    df = pd.read_csv(io.StringIO(file_string),sep=';')
    return df
    
def api_extracion(url,authorization=None)->pd.DataFrame:    
    """
    Make a get request to an api and returns the response as a DataFrame.

    Args:
        url (str): Url of the api.
        authorization (str): token authorization if needed.

    Returns:
        pd.DataFrame: DataFrame containing the data.
    """
    payload={}
    if authorization:
        headers = {
        'Authorization': authorization
        }
        try:
            response = requests.request("GET", url, headers=headers, data=payload)
        except ConnectionError as e:
            raise f'{e},conection failed with api {url}'
    else:
        try:
            response = requests.request("GET", url, data=payload)
        except ConnectionError as e:
            raise f'{e},conection failed with api {url}'
    df = pd.read_csv(io.StringIO(response.content.decode('utf-8')),sep=';')
    return df


def full_extraction():
    '''
    
    This function fetch the information from diferent places and send them to google big query:

    Steps:
        1. Connects to a PostgreSQL database and fetches data from two tables, mineria_almacen and mineria_producto, and creates Pandas DataFrames from the fetched results.
        2. Fetches data from a CSV file named mineria_venta.csv and creates a Pandas DataFrame.
        3. Fetches data from a CSV file named mineria_resultado_campanas.csv and creates a Pandas DataFrame.
        4. Sends a GET request to a URL with an authorization token to download a CSV file, and creates a Pandas DataFrame from the downloaded content.
        5. Defines table IDs for BigQuery tables where the data will be uploaded.
        6. Uploads the DataFrames to BigQuery tables using the pandas_gbq library, parameter set to 'replace', which means the tables will be replaced if they already exist.
    Returns:None
    '''
    postgresql=Postgresql_fecth_excecutor()
    df_mineria_producto=postgresql.download_table('mineria_producto')
    df_mineria_almacen=postgresql.download_table('mineria_almacen')
    postgresql.close_conection()

    df_mineria_ventas_csv=download_csv_from_gcs('customer_experience_bucket', 'mineria_venta.csv','customer experience')
    df_mineria_resultado_campanas_csv=download_csv_from_gcs('customer_experience_bucket', 'mineria_venta.csv','customer experience')
    

    url="https://v1-api.lax.marketing/api/blacklist/downloadFile"
    authorization='Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZFVzZXIiOjg5LCJpZENvbXBhbnkiOjQzLCJyb2xVc2VyIjoyLCJpYXQiOjE2ODIwODM2NDN9.HvKEkVpeSmC3xnfq9HFzpT2usSewnTfBDkOVNMQ2k9Q'
    df_blacklist=api_extracion(url,authorization)


    table_id_blacklist = 'customer-experience-384423.Data_cruda.blacklist'
    table_id_mineria_ventas_csv = 'customer-experience-384423.Data_cruda.mineria_venta'
    table_id_mineria_resultado_campanas_csv = 'customer-experience-384423.Data_cruda.mineria_resultado_campanas'
    table_id_mineria_producto = 'customer-experience-384423.Data_cruda.mineria_producto'
    table_id_mineria_almacen= 'customer-experience-384423.Data_cruda.mineria_almacen'
    project_id='customer-experience-384423'
    
    pandas_gbq.to_gbq(df_blacklist.astype('string'), table_id_blacklist, project_id=project_id, if_exists='replace')
    pandas_gbq.to_gbq(df_mineria_ventas_csv.astype('string'), table_id_mineria_ventas_csv, project_id=project_id, if_exists='replace')
    pandas_gbq.to_gbq(df_mineria_resultado_campanas_csv.astype('string'), table_id_mineria_resultado_campanas_csv, project_id=project_id, if_exists='replace')
    pandas_gbq.to_gbq(df_mineria_producto.astype('string'), table_id_mineria_producto, project_id=project_id, if_exists='replace')
    pandas_gbq.to_gbq(df_mineria_almacen.astype('string'), table_id_mineria_almacen, project_id=project_id, if_exists='replace')


