
import os
import sys
import pandas as pd
from datetime import datetime
from dateutil import parser
from utils_etl.load_bq import load_to_bq, connect_to_bigquery
from utils_etl.extract_pg import connect_to_pg, list_tables, list_columns, postgresql_to_dataframe
from connections.params_dic import params_dic


def validate_date(date: str) -> bool:
    # using try-except to check for truth value
    res = True
    try:
        res = bool(parser.isoparse(date))
    except ValueError:
        res = False
    
    return res

def extract_data(date: str):
    ## Extract pgSQL
    conn = connect_to_pg(params_dic)
    tables = list_tables(conn)

    for table in tables:
        path = f"northwind_local/{table}/{date}/"
        if not os.path.exists(path):
            os.makedirs(path)
        
        column_names = list_columns(conn,table)
        df = postgresql_to_dataframe(conn, table, column_names)
        
        df.to_csv(path + f"{table}.csv",index=False)
        print(f"{table} csv successfully saved in local disk")

    ## Extract .csv file
    path_csv = f"northwind_local/order_details/{date}/"
    if not os.path.exists(path_csv):
        os.makedirs(path_csv)
    df_csv = pd.read_csv('./data/order_details.csv')
    df_csv.to_csv(path_csv + f"order_details.csv",index=False)

    return f"\nStep 1 done successfully to date: {date} \n"


def load_data(date: str):
    #load to Bquery in GCloud
    client = connect_to_bigquery()
    table = ""
    for dirs, subdirs, files in os.walk('northwind_local'):
        if date in dirs:
            for file in files:
                local_path = os.path.join(dirs, file)
                df_local = pd.read_csv(local_path)
                table = file.replace('.csv','')
                print(load_to_bq(client,table,df_local))

    if table == "":
        return f"\nThere are no records for this date: {date} \n"

    return f"\nStep 2 done successfully to date: {date} \n"

def main(argv):

    params = sys.argv[1:]
    ## Validating date
    if params:
        if len(params) == 2:
            date = params[1]
            if not validate_date(date):
                print("invalid date, please put the date in the format:\nYYYY-MM-DD")
                sys.exit(1)
        else:
            date = date = datetime.now().strftime("%Y-%m-%d")


    # getting run parameters
    if "-e" in params:
        print(extract_data(date))

    elif "-l" in params:
        print(load_data(date))

    elif "-enl" in params:
        print(extract_data(date))
        print(load_data(date))

    else:
        print("\nExecution pattern: py main.py <param 1> <param 2>\n")
        print("param 1 (mandatory):\n<-e> to run only step 1 \n<-l> to run only step 2 \n<-enl> to run steps 1 and 2")
        print("\nparam 2 (optional): <YYYY-MM-DD>")
        print("If <date YYYY-MM-DD> was not defined, its will consider current date\n")


if __name__ == "__main__":
   main(sys.argv[1:])