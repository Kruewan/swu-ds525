import json
import glob
import os
from typing import List

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files

    
def _drop_tables():
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    table_drop_events = "DROP TABLE IF EXISTS events"
    table_drop_actors = "DROP TABLE IF EXISTS actors"
    table_drop_repos = "DROP TABLE IF EXISTS Repo"
    table_drop_payloads = "DROP TABLE IF EXISTS Payload"
    table_drop_orgs = "DROP TABLE IF EXISTS Org"

    drop_table_queries = [
        table_drop_events,
        table_drop_actors,
        table_drop_repos,
        table_drop_payloads,
        table_drop_orgs,
    ]
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def _create_tables():
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    table_create_actors = """
        CREATE TABLE IF NOT EXISTS actors (
            actorId INT NOT NULL,
            login VARCHAR(255),
            display_login VARCHAR(255),
            gravatar_id VARCHAR(255),
            url VARCHAR(255),
            avatar_url VARCHAR(255),
            PRIMARY KEY (actorId)
        )
    """
    table_create_events = """
        CREATE TABLE IF NOT EXISTS events (
            eventId TEXT NOT NULL,
            type VARCHAR(255),
            actorId INT NOT NULL,
            repoId INT NOT NULL,
            action VARCHAR(255),
            public BOOLEAN,
            created_at TIMESTAMP,
            orgId INT,
            PRIMARY KEY (eventId),
            CONSTRAINT fk_actor FOREIGN KEY (actorId)
                REFERENCES actors (actorId),
            CONSTRAINT fk_repo FOREIGN KEY (repoId)
                REFERENCES Repo (repoId)
        )
    """
    table_create_repos = """
        CREATE TABLE IF NOT EXISTS Repo (
            repoId INT NOT NULL,
            name VARCHAR(255),
            url VARCHAR(255),
            PRIMARY KEY (repoId)
        )
    """
    table_create_payloads = """
        CREATE TABLE IF NOT EXISTS Payload (
            push_id INT,
            action VARCHAR(255),
            ref VARCHAR(255),
            author_email VARCHAR(255),
            author_name VARCHAR(255),
            message VARCHAR(255),
            distincts BOOLEAN,
            url VARCHAR(255)
        )
    """
    table_create_orgs = """
        CREATE TABLE IF NOT EXISTS Org (
            orgId INT NOT NULL,
            login VARCHAR(255),
            gravatar_id VARCHAR(255),
            url VARCHAR(255),
            avatar_url VARCHAR(255),
            PRIMARY KEY (orgId)
        )
    """

    create_table_queries = [
        table_create_actors,
        table_create_repos,
        table_create_payloads,
        table_create_orgs,
        table_create_events,
    ]
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def _process(**context):
    hook = PostgresHook(postgres_conn_id="my_postgres")
    conn = hook.get_conn()
    cur = conn.cursor()

    ti = context["ti"]

    # Get list of files from filepath
    all_files = ti.xcom_pull(task_ids="get_files", key="return_value")
    # all_files = get_files(filepath)

    i = 0
    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                
                # Insert data into tables here
                
                insert_statement_actor = f"""
                    INSERT INTO actors (actorId,login,display_login,gravatar_id,url,avatar_url)
                    VALUES ('{each["actor"]["id"]}'
                            , '{each["actor"]["login"]}'
                            , '{each["actor"]["display_login"]}'
                            , '{each["actor"]["gravatar_id"]}'
                            , '{each["actor"]["url"]}'
                            , '{each["actor"]["avatar_url"]}')
                    ON CONFLICT (actorId) DO NOTHING
                """
                cur.execute(insert_statement_actor)

                insert_statement_repo = f"""
                    INSERT INTO Repo (repoId,name,url)
                    VALUES ('{each["repo"]["id"]}'
                            , '{each["repo"]["name"]}'
                            , '{each["repo"]["url"]}')
                    ON CONFLICT (repoId) DO NOTHING
                """
                cur.execute(insert_statement_repo)

                try:
                    insert_statement_org = f"""
                        INSERT INTO Org (orgId,login,gravatar_id,url,avatar_url)
                        VALUES ('{each["org"]["id"]}'
                                , '{each["org"]["login"]}'
                                , '{each["org"]["gravatar_id"]}'
                                , '{each["org"]["url"]}'
                                , '{each["org"]["avatar_url"]}')
                        ON CONFLICT (orgId) DO NOTHING
                    """
                    cur.execute(insert_statement_org)
                except:
                    pass

                try:
                    insert_statement_event = f"""
                        INSERT INTO events (eventId,type,actorId,repoId,public,created_at,orgId) 
                        VALUES ('{each["id"]}'
                                , '{each["type"]}'
                                , '{each["actor"]["id"]}'
                                , '{each["repo"]["id"]}'
                                , '{each["public"]}'
                                , '{each["created_at"]}'
                                , '{each["org"]["id"]}'
                                )
                        ON CONFLICT (eventId) DO NOTHING
                    """
                    cur.execute(insert_statement_event)
                except:
                    pass

               
                try:
                    conn.commit()
                    i = i+1

                except:
                    # Rolling back in case of error
                    conn.rollback()
    
    print("Data inserted total " + str(i) + " records")


with DAG(
    "etl",
    start_date=timezone.datetime(2022, 10, 15),
    schedule="@daily",
    tags=["workshop"],
    catchup=False,
) as dag:

    get_files = PythonOperator(
        task_id="get_files",
        python_callable=_get_files,
        op_kwargs={
            "filepath": "/opt/airflow/dags/data",
        }
    )
    
    drop_tables = PythonOperator(
        task_id="drop_tables",
        python_callable=_drop_tables,
    )

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=_create_tables,
    )
    
    process = PythonOperator(
        task_id="process",
        python_callable=_process,
    )

    drop_tables >> [get_files, create_tables] >> process