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

    aws_access_key_id = ASIA46YTXNWJI2PQSNOC
    aws_secret_access_key = c7u6/BTOJY1o7HiY3AsOTmHyB4lHk9znZXcqbtt1
    aws_session_token = FwoGZXIvYXdzEFsaDH5LTm6wdrUJt1/G/yLIAaZGcgQ5NYTAvL1WMK8uQTRH4IYGY75vnVIBzWcnsfC9DS/YO5iIs9qRlWoxsvPn6LfDo7x9RJhHm1sXafKNoSoz8l/v0q1UWN94Ez4IUV3HczuHF/J5bARU7FBw2n7W+zTtMlwj4Grn+mdVHbtYfdyngrEpKgzc1CYKLOrVZB1pvha4nuWe+ycIzOqomZ2DIDfjuq1uK7cGqEne8bQkpuzSE3TwuMY1Zsyq2FO5TSqW8kyOuUWmwe2S76vyxK5BNSxEh4ndyeu1KPyX0ZwGMi3Zol/pNOteafYs1j5H0pJEiP13wgworPVjeMb861eTu/FFDRpUrqB7kzbGHt8

    client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token
    )
    print(client)

    response = client.list_objects(
        Bucket="zkan-swu-labs",
        MaxKeys=2,
    )

    contents = response["Contents"]
    for content in contents:
        print(content["Key"], content["Size"])



def _get_files():

    copy_table_queries = [
        """
        COPY events FROM 's3://juneawsbucket/github_events_01.json'
        CREDENTIALS 'aws_iam_role=arn:aws:iam::890710224274:role/LabRole'
        JSON 's3://juneawsbucket/events_json_path.json'
        REGION 'us-east-1'
        """,
    ]
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

    
def _drop_tables():
    hook = PostgresHook(postgres_conn_id="my_postgres")
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
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    table_create_accidents = """
        CREATE TABLE IF NOT EXISTS accidents (
            id INT NOT NULL,
            accident_date TIMESTAMP,
            accident_time TIMESTAMP,
            expw_step VARCHAR(255),
            weather_state VARCHAR(255),
            injur_man INT,
            injur_femel INT,
            dead_man INT,
            dead_femel INT,
            cause VARCHAR(255),
            PRIMARY KEY (id)
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
    start_date=timezone.datetime(2022, 10, 15),
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
    
    upload_files

    #drop_tables >> [get_files, create_tables] >> insert_tables