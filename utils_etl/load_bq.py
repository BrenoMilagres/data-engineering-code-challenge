import sys
import warnings
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

## ignorando warnings de tipos das colunas
warnings.filterwarnings("ignore")


def connect_to_bigquery():
    try:
        print('Connecting to the Bigquery...')
        key_path = 'connections/chave_acesso_bquery.json'
        credentials = service_account.Credentials.from_service_account_file(
            key_path, 
            scopes = ['https://www.googleapis.com/auth/cloud-platform']
        )

        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        print("Connection successful \n")
    except Exception as error:
        print("Error in load_to_bq(): %s" % error)
        sys.exit(1) 

    return client


def load_to_bq(client: bigquery.Client, table: str, df: pd.DataFrame) -> str:
    # Estabelendo conexão com Bquery através de uma chave de acesso .json que está
    # vinculada a uma conta de serviço com as permissões necessárias na GCloud
    try:
        table_id = f"northwind_gcp.{table}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )  
        job.result()
    except Exception as error:
        print("Error in load_to_bq(): %s" % error)
        sys.exit(1) 

    return f"loaded {df.shape[0]} rows to {table_id}" 

    