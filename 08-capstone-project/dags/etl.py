import json
import glob
import os
#import psycopg2
import boto3
from typing import List
from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



def _upload_files():

    aws_access_key_id = "ASIA46YTXNWJI2PQSNOC"
    aws_secret_access_key = "c7u6/BTOJY1o7HiY3AsOTmHyB4lHk9znZXcqbtt1"
    aws_session_token = "FwoGZXIvYXdzEFsaDH5LTm6wdrUJt1/G/yLIAaZGcgQ5NYTAvL1WMK8uQTRH4IYGY75vnVIBzWcnsfC9DS/YO5iIs9qRlWoxsvPn6LfDo7x9RJhHm1sXafKNoSoz8l/v0q1UWN94Ez4IUV3HczuHF/J5bARU7FBw2n7W+zTtMlwj4Grn+mdVHbtYfdyngrEpKgzc1CYKLOrVZB1pvha4nuWe+ycIzOqomZ2DIDfjuq1uK7cGqEne8bQkpuzSE3TwuMY1Zsyq2FO5TSqW8kyOuUWmwe2S76vyxK5BNSxEh4ndyeu1KPyX0ZwGMi3Zol/pNOteafYs1j5H0pJEiP13wgworPVjeMb861eTu/FFDRpUrqB7kzbGHt8"

    s3 = boto3.resource(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token
    )

    s3.meta.client.upload_file(
        "/opt/airflow/dags/data/accidentmonth.csv", 
        "junnieebucket", 
        "accidentmonth.csv",
    )



def _get_files():
    hook = PostgresHook(postgres_conn_id="my_redshift")
    conn = hook.get_conn()
    cur = conn.cursor()

    copy_table_queries = [
        """
        COPY accidents FROM 's3://junnieebucket/accidentmonth.csv'
        ACCESS_KEY_ID 'ASIA46YTXNWJI2PQSNOC'
        SECRET_ACCESS_KEY 'c7u6/BTOJY1o7HiY3AsOTmHyB4lHk9znZXcqbtt1'
        SESSION_TOKEN 'FwoGZXIvYXdzEFsaDH5LTm6wdrUJt1/G/yLIAaZGcgQ5NYTAvL1WMK8uQTRH4IYGY75vnVIBzWcnsfC9DS/YO5iIs9qRlWoxsvPn6LfDo7x9RJhHm1sXafKNoSoz8l/v0q1UWN94Ez4IUV3HczuHF/J5bARU7FBw2n7W+zTtMlwj4Grn+mdVHbtYfdyngrEpKgzc1CYKLOrVZB1pvha4nuWe+ycIzOqomZ2DIDfjuq1uK7cGqEne8bQkpuzSE3TwuMY1Zsyq2FO5TSqW8kyOuUWmwe2S76vyxK5BNSxEh4ndyeu1KPyX0ZwGMi3Zol/pNOteafYs1j5H0pJEiP13wgworPVjeMb861eTu/FFDRpUrqB7kzbGHt8'
        CSV
        ignoreheader 1
        ACCEPTINVCHARS AS '_'
        REGION 'us-east-1'
        """,
    ]
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

     
def _drop_tables():
    hook = PostgresHook(postgres_conn_id="my_redshift")
    conn = hook.get_conn()
    cur = conn.cursor()

    table_drop_accidents = "DROP TABLE IF EXISTS accidents"

    drop_table_queries = [
        table_drop_accidents
    ]
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def _create_tables():
    hook = PostgresHook(postgres_conn_id="my_redshift")
    conn = hook.get_conn()
    cur = conn.cursor()

    table_create_accidents = """
        CREATE TABLE IF NOT EXISTS accidents (
           
            accident_date VARCHAR(255),
            accident_time VARCHAR(255),
            expw_step VARCHAR(255),
            weather_state VARCHAR(255),
            injur_man VARCHAR(255),
            injur_femel VARCHAR(255),
            dead_man VARCHAR(255),
            dead_femel VARCHAR(255),
            cause VARCHAR(255)
        )
    """
    
    create_table_queries = [
        table_create_accidents
    ]
    
    for query in create_table_queries:
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

    get_files = PythonOperator(
        task_id="get_files",
        python_callable=_get_files,
    )
    
    drop_tables = PythonOperator(
        task_id="drop_tables",
        python_callable=_drop_tables,
    )

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=_create_tables,
    )

    insert_tables = PythonOperator(
        task_id="insert_tables",
        python_callable=_insert_tables,
    )
    
  
    #drop_tables >> [get_files, create_tables] >> insert_tables

    upload_files >> drop_tables >> create_tables >> get_files
