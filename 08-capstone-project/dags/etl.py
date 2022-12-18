import json
import glob
import os
#import psycopg2
import boto3
import csv
import pandas as pd
from typing import List
from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



def _upload_files():

    aws_access_key_id = "ASIA46YTXNWJMAF6LMDS"
    aws_secret_access_key = "NMbmnE1DJXoCuwt32ftTwKfq1Mpbr05xYCN9wT0V"
    aws_session_token = "FwoGZXIvYXdzEBMaDM34gwp8FTEATq1UayLIAYA6l/WNhVUhAmJ9WAHaDaf0j+Pi2IpMkzP3XtHU8hI2Lq/GGQ7LhWE/PppP8rpjKk9j6Vdn6wR4s/QuBoBtMEQEEWIe7XtymDmHmlAIng3Covn2cp3AI9GU7yfdf04Y8/IbJDSQTlLlVZy97WfycrGwqssDppPPC7t8zuNLbQOYPrjAt+8zKcxXE07SwfrrWOw3hiWAKBLLn69C+T8UX5Dof7PhIUb0xilXo9x6/db1SwFDlQLUgBLiNIX2BannbaW76n5KBkTRKJDP+ZwGMi2n03LCPJLASRoc7oTOwRkPZHvLLWvcn2oOwXOAh3FiyyMaViM/wSzTVmU4CdI"
            
    s3 = boto3.resource(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token
    )

    s3.meta.client.upload_file(
        "/opt/airflow/dags/data/accident_2559.csv", 
        "junnieebucket", 
        "accident_2559.csv",
    )

    s3.meta.client.upload_file(
        "/opt/airflow/dags/data/accident_2560.csv", 
        "junnieebucket", 
        "accident_2560.csv",
    )

    s3.meta.client.upload_file(
        "/opt/airflow/dags/data/accident_2561.csv", 
        "junnieebucket", 
        "accident_2561.csv",
    )

    s3.meta.client.upload_file(
        "/opt/airflow/dags/data/accident_2562.csv", 
        "junnieebucket", 
        "accident_2562.csv",
    )

    s3.meta.client.upload_file(
        "/opt/airflow/dags/data/accident_2563.csv", 
        "junnieebucket", 
        "accident_2563.csv",
    )

    s3.meta.client.upload_file(
        "/opt/airflow/dags/data/accident_2564.csv", 
        "junnieebucket", 
        "accident_2564.csv",
    )

    s3.meta.client.upload_file(
        "/opt/airflow/dags/data/accident_2565.csv", 
        "junnieebucket", 
        "accident_2565.csv",
    )



def _create_tables():
    hook = PostgresHook(postgres_conn_id="my_redshift")
    conn = hook.get_conn()
    cur = conn.cursor()

    table_create_accident_2559 = """
        CREATE TABLE IF NOT EXISTS accident_2559 (
            accident_date VARCHAR(10),
            accident_time VARCHAR(10),
            expw_step VARCHAR(255),
            weather_state VARCHAR(255),
            injur_man int,
            injur_femel int,
            dead_man int,
            dead_femel int,
            cause VARCHAR(255)
        )
    """
    
    table_create_accident_2560 = """
        CREATE TABLE IF NOT EXISTS accident_2560 (
            accident_date VARCHAR(10),
            accident_time VARCHAR(10),
            expw_step VARCHAR(255),
            weather_state VARCHAR(255),
            injur_man int,
            injur_femel int,
            dead_man int,
            dead_femel int,
            cause VARCHAR(255)
        )
    """

    table_create_accident_2561 = """
        CREATE TABLE IF NOT EXISTS accident_2561 (
            accident_date VARCHAR(10),
            accident_time VARCHAR(10),
            expw_step VARCHAR(255),
            weather_state VARCHAR(255),
            injur_man int,
            injur_femel int,
            dead_man int,
            dead_femel int,
            cause VARCHAR(255)
        )
    """

    table_create_accident_2562 = """
        CREATE TABLE IF NOT EXISTS accident_2562 (
            accident_date VARCHAR(10),
            accident_time VARCHAR(10),
            expw_step VARCHAR(255),
            weather_state VARCHAR(255),
            injur_man int,
            injur_femel int,
            dead_man int,
            dead_femel int,
            cause VARCHAR(255)
        )
    """

    table_create_accident_2563 = """
        CREATE TABLE IF NOT EXISTS accident_2563 (
            accident_date VARCHAR(10),
            accident_time VARCHAR(10),
            expw_step VARCHAR(255),
            weather_state VARCHAR(255),
            injur_man int,
            injur_femel int,
            dead_man int,
            dead_femel int,
            cause VARCHAR(255)
        )
    """

    table_create_accident_2564 = """
        CREATE TABLE IF NOT EXISTS accident_2564 (
            accident_date VARCHAR(10),
            accident_time VARCHAR(10),
            expw_step VARCHAR(255),
            weather_state VARCHAR(255),
            injur_man int,
            injur_femel int,
            dead_man int,
            dead_femel int,
            cause VARCHAR(255)
        )
    """

    table_create_accident_2565 = """
        CREATE TABLE IF NOT EXISTS accident_2565 (
            accident_date VARCHAR(10),
            accident_time VARCHAR(10),
            expw_step VARCHAR(255),
            weather_state VARCHAR(255),
            injur_man int,
            injur_femel int,
            dead_man int,
            dead_femel int,
            cause VARCHAR(255)
        )
    """
    
    create_table_queries = [
        table_create_accident_2559,
        table_create_accident_2560,
        table_create_accident_2561,
        table_create_accident_2562,
        table_create_accident_2563,
        table_create_accident_2564,
        table_create_accident_2565,
    ]
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()



def _delete_tables():
    hook = PostgresHook(postgres_conn_id="my_redshift")
    conn = hook.get_conn()
    cur = conn.cursor()

    table_drop_accident_2559 = "DELETE FROM  accident_2559"
    table_drop_accident_2560 = "DELETE FROM  accident_2560"
    table_drop_accident_2561 = "DELETE FROM  accident_2561"
    table_drop_accident_2562 = "DELETE FROM  accident_2562"
    table_drop_accident_2563 = "DELETE FROM  accident_2563"
    table_drop_accident_2564 = "DELETE FROM  accident_2564"
    table_drop_accident_2565 = "DELETE FROM  accident_2565"

    drop_table_queries = [
        table_drop_accident_2559,
        table_drop_accident_2560,
        table_drop_accident_2561,
        table_drop_accident_2562,
        table_drop_accident_2563,
        table_drop_accident_2564,
        table_drop_accident_2565,
    ]
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()



def _get_files():
    hook = PostgresHook(postgres_conn_id="my_redshift")
    conn = hook.get_conn()
    cur = conn.cursor()

    copy_table_accident_2559 = """
        COPY accident_2559 FROM 's3://junnieebucket/accident_2559.csv'
        ACCESS_KEY_ID 'ASIA46YTXNWJMAF6LMDS'
        SECRET_ACCESS_KEY 'NMbmnE1DJXoCuwt32ftTwKfq1Mpbr05xYCN9wT0V'
        SESSION_TOKEN 'FwoGZXIvYXdzEBMaDM34gwp8FTEATq1UayLIAYA6l/WNhVUhAmJ9WAHaDaf0j+Pi2IpMkzP3XtHU8hI2Lq/GGQ7LhWE/PppP8rpjKk9j6Vdn6wR4s/QuBoBtMEQEEWIe7XtymDmHmlAIng3Covn2cp3AI9GU7yfdf04Y8/IbJDSQTlLlVZy97WfycrGwqssDppPPC7t8zuNLbQOYPrjAt+8zKcxXE07SwfrrWOw3hiWAKBLLn69C+T8UX5Dof7PhIUb0xilXo9x6/db1SwFDlQLUgBLiNIX2BannbaW76n5KBkTRKJDP+ZwGMi2n03LCPJLASRoc7oTOwRkPZHvLLWvcn2oOwXOAh3FiyyMaViM/wSzTVmU4CdI'
        CSV
        IGNOREHEADER 1
        REGION 'us-east-1'
        """

    copy_table_accident_2560 = """
        COPY accident_2560 FROM 's3://junnieebucket/accident_2560.csv'
        ACCESS_KEY_ID 'ASIA46YTXNWJMAF6LMDS'
        SECRET_ACCESS_KEY 'NMbmnE1DJXoCuwt32ftTwKfq1Mpbr05xYCN9wT0V'
        SESSION_TOKEN 'FwoGZXIvYXdzEBMaDM34gwp8FTEATq1UayLIAYA6l/WNhVUhAmJ9WAHaDaf0j+Pi2IpMkzP3XtHU8hI2Lq/GGQ7LhWE/PppP8rpjKk9j6Vdn6wR4s/QuBoBtMEQEEWIe7XtymDmHmlAIng3Covn2cp3AI9GU7yfdf04Y8/IbJDSQTlLlVZy97WfycrGwqssDppPPC7t8zuNLbQOYPrjAt+8zKcxXE07SwfrrWOw3hiWAKBLLn69C+T8UX5Dof7PhIUb0xilXo9x6/db1SwFDlQLUgBLiNIX2BannbaW76n5KBkTRKJDP+ZwGMi2n03LCPJLASRoc7oTOwRkPZHvLLWvcn2oOwXOAh3FiyyMaViM/wSzTVmU4CdI'
        CSV
        IGNOREHEADER 1
        REGION 'us-east-1'
        """

    copy_table_accident_2561 = """
        COPY accident_2561 FROM 's3://junnieebucket/accident_2561.csv'
        ACCESS_KEY_ID 'ASIA46YTXNWJMAF6LMDS'
        SECRET_ACCESS_KEY 'NMbmnE1DJXoCuwt32ftTwKfq1Mpbr05xYCN9wT0V'
        SESSION_TOKEN 'FwoGZXIvYXdzEBMaDM34gwp8FTEATq1UayLIAYA6l/WNhVUhAmJ9WAHaDaf0j+Pi2IpMkzP3XtHU8hI2Lq/GGQ7LhWE/PppP8rpjKk9j6Vdn6wR4s/QuBoBtMEQEEWIe7XtymDmHmlAIng3Covn2cp3AI9GU7yfdf04Y8/IbJDSQTlLlVZy97WfycrGwqssDppPPC7t8zuNLbQOYPrjAt+8zKcxXE07SwfrrWOw3hiWAKBLLn69C+T8UX5Dof7PhIUb0xilXo9x6/db1SwFDlQLUgBLiNIX2BannbaW76n5KBkTRKJDP+ZwGMi2n03LCPJLASRoc7oTOwRkPZHvLLWvcn2oOwXOAh3FiyyMaViM/wSzTVmU4CdI'
        CSV
        IGNOREHEADER 1
        REGION 'us-east-1'
        """

    copy_table_accident_2562 = """
        COPY accident_2562 FROM 's3://junnieebucket/accident_2562.csv'
        ACCESS_KEY_ID 'ASIA46YTXNWJMAF6LMDS'
        SECRET_ACCESS_KEY 'NMbmnE1DJXoCuwt32ftTwKfq1Mpbr05xYCN9wT0V'
        SESSION_TOKEN 'FwoGZXIvYXdzEBMaDM34gwp8FTEATq1UayLIAYA6l/WNhVUhAmJ9WAHaDaf0j+Pi2IpMkzP3XtHU8hI2Lq/GGQ7LhWE/PppP8rpjKk9j6Vdn6wR4s/QuBoBtMEQEEWIe7XtymDmHmlAIng3Covn2cp3AI9GU7yfdf04Y8/IbJDSQTlLlVZy97WfycrGwqssDppPPC7t8zuNLbQOYPrjAt+8zKcxXE07SwfrrWOw3hiWAKBLLn69C+T8UX5Dof7PhIUb0xilXo9x6/db1SwFDlQLUgBLiNIX2BannbaW76n5KBkTRKJDP+ZwGMi2n03LCPJLASRoc7oTOwRkPZHvLLWvcn2oOwXOAh3FiyyMaViM/wSzTVmU4CdI'
        CSV
        IGNOREHEADER 1
        REGION 'us-east-1'
        """

    copy_table_accident_2563 = """
        COPY accident_2563 FROM 's3://junnieebucket/accident_2563.csv'
        ACCESS_KEY_ID 'ASIA46YTXNWJMAF6LMDS'
        SECRET_ACCESS_KEY 'NMbmnE1DJXoCuwt32ftTwKfq1Mpbr05xYCN9wT0V'
        SESSION_TOKEN 'FwoGZXIvYXdzEBMaDM34gwp8FTEATq1UayLIAYA6l/WNhVUhAmJ9WAHaDaf0j+Pi2IpMkzP3XtHU8hI2Lq/GGQ7LhWE/PppP8rpjKk9j6Vdn6wR4s/QuBoBtMEQEEWIe7XtymDmHmlAIng3Covn2cp3AI9GU7yfdf04Y8/IbJDSQTlLlVZy97WfycrGwqssDppPPC7t8zuNLbQOYPrjAt+8zKcxXE07SwfrrWOw3hiWAKBLLn69C+T8UX5Dof7PhIUb0xilXo9x6/db1SwFDlQLUgBLiNIX2BannbaW76n5KBkTRKJDP+ZwGMi2n03LCPJLASRoc7oTOwRkPZHvLLWvcn2oOwXOAh3FiyyMaViM/wSzTVmU4CdI'
        CSV
        IGNOREHEADER 1
        REGION 'us-east-1'
        """

    copy_table_accident_2564 = """
        COPY accident_2564 FROM 's3://junnieebucket/accident_2564.csv'
        ACCESS_KEY_ID 'ASIA46YTXNWJMAF6LMDS'
        SECRET_ACCESS_KEY 'NMbmnE1DJXoCuwt32ftTwKfq1Mpbr05xYCN9wT0V'
        SESSION_TOKEN 'FwoGZXIvYXdzEBMaDM34gwp8FTEATq1UayLIAYA6l/WNhVUhAmJ9WAHaDaf0j+Pi2IpMkzP3XtHU8hI2Lq/GGQ7LhWE/PppP8rpjKk9j6Vdn6wR4s/QuBoBtMEQEEWIe7XtymDmHmlAIng3Covn2cp3AI9GU7yfdf04Y8/IbJDSQTlLlVZy97WfycrGwqssDppPPC7t8zuNLbQOYPrjAt+8zKcxXE07SwfrrWOw3hiWAKBLLn69C+T8UX5Dof7PhIUb0xilXo9x6/db1SwFDlQLUgBLiNIX2BannbaW76n5KBkTRKJDP+ZwGMi2n03LCPJLASRoc7oTOwRkPZHvLLWvcn2oOwXOAh3FiyyMaViM/wSzTVmU4CdI'
        CSV
        IGNOREHEADER 1
        REGION 'us-east-1'
        """

    copy_table_accident_2565 = """
        COPY accident_2565 FROM 's3://junnieebucket/accident_2565.csv'
        ACCESS_KEY_ID 'ASIA46YTXNWJMAF6LMDS'
        SECRET_ACCESS_KEY 'NMbmnE1DJXoCuwt32ftTwKfq1Mpbr05xYCN9wT0V'
        SESSION_TOKEN 'FwoGZXIvYXdzEBMaDM34gwp8FTEATq1UayLIAYA6l/WNhVUhAmJ9WAHaDaf0j+Pi2IpMkzP3XtHU8hI2Lq/GGQ7LhWE/PppP8rpjKk9j6Vdn6wR4s/QuBoBtMEQEEWIe7XtymDmHmlAIng3Covn2cp3AI9GU7yfdf04Y8/IbJDSQTlLlVZy97WfycrGwqssDppPPC7t8zuNLbQOYPrjAt+8zKcxXE07SwfrrWOw3hiWAKBLLn69C+T8UX5Dof7PhIUb0xilXo9x6/db1SwFDlQLUgBLiNIX2BannbaW76n5KBkTRKJDP+ZwGMi2n03LCPJLASRoc7oTOwRkPZHvLLWvcn2oOwXOAh3FiyyMaViM/wSzTVmU4CdI'
        CSV
        IGNOREHEADER 1
        REGION 'us-east-1'
        """

    copy_table_queries = [
        copy_table_accident_2559,
        copy_table_accident_2560,
        copy_table_accident_2561,
        copy_table_accident_2562,
        copy_table_accident_2563,
        copy_table_accident_2565,
    ]
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def _redshift_to_dataframe():

    # Get data from Redshift
    hook = PostgresHook(postgres_conn_id="my_redshift")
    conn = hook.get_conn()
    cur = conn.cursor()

    table_select_events_accidents_total = """ SELECT * FROM events_accidents_total """
    cur.execute(table_select_events_accidents_total)
    df = pd.DataFrame(cur.fetchall())
    df.to_csv (r'/opt/airflow/dags/data/download/events_accidents_total.csv', index = False) 

    table_select_events_accidents_count_total = """ SELECT * FROM events_accidents_count_total """
    cur.execute(table_select_events_accidents_count_total)
    df = pd.DataFrame(cur.fetchall())
    df.to_csv (r'/opt/airflow/dags/data/download/events_accidents_count_total.csv', index = False) 

  
     
def _drop_tables():
    hook = PostgresHook(postgres_conn_id="my_redshift")
    conn = hook.get_conn()
    cur = conn.cursor()

    table_drop_accidents = "DROP TABLE accidents cascade"

    drop_table_queries = [
        table_drop_accidents
    ]
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()



def _insert_tables():
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    table_insert_accidents = """
        INSERT INTO
            accidents_staging (
                id, expw_step, cause 
            )
            SELECT
                id, expw_step, cause
            FROM
                accidents
            WHERE
                id NOT IN (SELECT DISTINCT id FROM accidents_staging)
    """
    
    insert_table_queries = [
        table_insert_accidents
    ]
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()



with DAG(
    "etl",
    start_date=timezone.datetime(2022, 12, 10),
    schedule="@daily",
    tags=["workshop"],
    catchup=False,
) as dag:

    upload_files = PythonOperator(
        task_id="upload_files",
        python_callable=_upload_files,
    )

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=_create_tables,
    )

    delete_tables = PythonOperator(
        task_id="delete_tables",
        python_callable=_delete_tables,
    )

    get_files = PythonOperator(
        task_id="get_files",
        python_callable=_get_files,
    )
    
    redshift_to_dataframe = PythonOperator(
        task_id="redshift_to_dataframe",
        python_callable=_redshift_to_dataframe,
    )

    #drop_tables = PythonOperator(
    #    task_id="drop_tables",
    #    python_callable=_drop_tables,
    #)

    
    upload_files >> create_tables >> delete_tables >> get_files >> redshift_to_dataframe
