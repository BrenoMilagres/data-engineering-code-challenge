import sys
import pandas as pd 
import psycopg2

from connections.params_dic import params_dic


def connect_to_pg(params_dic: dict) -> psycopg2.connect:

    conn = None
    try:
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error in connect(): %s" % error)
        sys.exit(1) 
    print("Connection successful \n")

    return conn


def list_tables(conn: psycopg2.connect) -> list:

    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT 
                table_name 
            FROM 
                information_schema.tables 
            WHERE 
                table_schema='public'
        """)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error in list_tables(): %s" % error)
        cursor.close()
        sys.exit(1) 

    tupples = cursor.fetchall()
    cursor.close()

    return list(pd.DataFrame(tupples)[0].unique())


def list_columns(conn: psycopg2.connect, table: str) -> list:

    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT
                column_name
            FROM
                information_schema.columns
            WHERE 
                table_name = '{table}'
        """)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error in list_columns(): %s" % error)
        cursor.close()
        sys.exit(1) 

    tupples = cursor.fetchall()
    cursor.close()

    return list(pd.DataFrame(tupples)[0].unique()) 


def postgresql_to_dataframe(conn: psycopg2.connect, table: str, columns_names: list) -> pd.DataFrame:

    columns_string = ', '.join(columns_names)
    select_query =  f"SELECT {columns_string} FROM {table}"

    cursor = conn.cursor()
    try:
        cursor.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error in postgresql_to_dataframe: %s" % error)
        cursor.close()
        sys.exit(1) 
    
    tupples = cursor.fetchall()
    cursor.close()
    
    return pd.DataFrame(tupples, columns=columns_names)









