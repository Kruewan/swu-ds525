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

    aws_access_key_id = "ASIA46YTXNWJFHSTWQ65"
    aws_secret_access_key = "E4681lIrRWPjbOw2mHkRGWQYx6P0ASgICrS0RcFZ"
    aws_session_token = "FwoGZXIvYXdzEAAaDHCwPduzkRT/y1HT+SLIAdyLE8ezr/AQDO0GvWCFk9Z3CEYm1nxtST+ccjexYvCimJMR7WZpZOtzUVYrSK6TG5+8Wsjp8I5rNZDbvgfrRvY8v6G7aOXtUMAHWi3Cc3FMixdj96ktBdwVzQwcQbUFQLAFRCtBh4d8c1Oh2S/CXB4ChEitf5Gfefy6eO/jbJ5guGFVHugEbBDAaRTdLfi4RpGshGzHfgqazIJRSTqPq37451sNkBpbgwB+uCnHGkzrdEbBes6ULsWByQdgJ+u1UCkDv9xkjJ/9KNzB9ZwGMi08sFPnWmum6Mbu/JFYXPDjdprCxSUz6Sb+ZKyJ0grh5Uel+52DgAiehsYAZck"

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
        ACCESS_KEY_ID 'ASIA46YTXNWJFHSTWQ65'
        SECRET_ACCESS_KEY 'E4681lIrRWPjbOw2mHkRGWQYx6P0ASgICrS0RcFZ'
        SESSION_TOKEN 'FwoGZXIvYXdzEAAaDHCwPduzkRT/y1HT+SLIAdyLE8ezr/AQDO0GvWCFk9Z3CEYm1nxtST+ccjexYvCimJMR7WZpZOtzUVYrSK6TG5+8Wsjp8I5rNZDbvgfrRvY8v6G7aOXtUMAHWi3Cc3FMixdj96ktBdwVzQwcQbUFQLAFRCtBh4d8c1Oh2S/CXB4ChEitf5Gfefy6eO/jbJ5guGFVHugEbBDAaRTdLfi4RpGshGzHfgqazIJRSTqPq37451sNkBpbgwB+uCnHGkzrdEbBes6ULsWByQdgJ+u1UCkDv9xkjJ/9KNzB9ZwGMi08sFPnWmum6Mbu/JFYXPDjdprCxSUz6Sb+ZKyJ0grh5Uel+52DgAiehsYAZck'
        CSV
        IGNOREHEADER 1
        REGION 'us-east-1'
        """,
    ]
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def _redshift_to_dataframe():

    # Get data from Redshift
    hook = PostgresHook(postgres_conn_id="my_redshift")
    conn = hook.get_conn()
    cur = conn.cursor()

    table_select_events_accident_date = """ SELECT * FROM events_accident_date """
    cur.execute(table_select_events_accident_date)
    df = pd.DataFrame(cur.fetchall())
    df.to_csv (r'/opt/airflow/dags/data/download/events_accident_date.csv', index = False) 

    table_select_events_cause = """ SELECT * FROM events_cause """
    cur.execute(table_select_events_cause)
    df = pd.DataFrame(cur.fetchall())
    df.to_csv (r'/opt/airflow/dags/data/download/events_cause.csv', index = False) 

    table_select_events_expw_step = """ SELECT * FROM events_expw_step """
    cur.execute(table_select_events_expw_step)
    df = pd.DataFrame(cur.fetchall())
    df.to_csv (r'/opt/airflow/dags/data/download/events_expw_step.csv', index = False) 

    table_select_events_total = """ SELECT * FROM events_total """
    cur.execute(table_select_events_total)
    df = pd.DataFrame(cur.fetchall())
    df.to_csv (r'/opt/airflow/dags/data/download/events_total.csv', index = False) 

    table_select_events_weather_state = """ SELECT * FROM events_weather_state """
    cur.execute(table_select_events_weather_state)
    df = pd.DataFrame(cur.fetchall())
    df.to_csv (r'/opt/airflow/dags/data/download/events_weather_state.csv', index = False) 

    table_select_events_cause_total = """ SELECT * FROM  events_cause_total """
    cur.execute(table_select_events_cause_total)
    df = pd.DataFrame(cur.fetchall())
    df.to_csv (r'/opt/airflow/dags/data/download/events_cause_total.csv', index = False) 

    table_select_events_expw_step_total = """ SELECT * FROM  events_expw_step_total """
    cur.execute(table_select_events_expw_step_total)
    df = pd.DataFrame(cur.fetchall())
    df.to_csv (r'/opt/airflow/dags/data/download/events_expw_step_total.csv', index = False) 

    table_select_events_weather_state_total = """ SELECT * FROM  events_weather_state_total """
    cur.execute(table_select_events_weather_state_total)
    df = pd.DataFrame(cur.fetchall())
    df.to_csv (r'/opt/airflow/dags/data/download/events_weather_state_total.csv', index = False) 

     
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


def _delete_tables():
    hook = PostgresHook(postgres_conn_id="my_redshift")
    conn = hook.get_conn()
    cur = conn.cursor()

    table_drop_accidents = "DELETE FROM  accidents"

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
    
    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=_create_tables,
    )

    redshift_to_dataframe = PythonOperator(
        task_id="redshift_to_dataframe",
        python_callable=_redshift_to_dataframe,
    )

    delete_tables = PythonOperator(
        task_id="delete_tables",
        python_callable=_delete_tables,
    )


    #drop_tables = PythonOperator(
    #    task_id="drop_tables",
    #    python_callable=_drop_tables,
    #)

    #insert_tables = PythonOperator(
    #    task_id="insert_tables",
    #    python_callable=_insert_tables,
    #)
    

    upload_files >> create_tables >> delete_tables >> get_files >> redshift_to_dataframe
