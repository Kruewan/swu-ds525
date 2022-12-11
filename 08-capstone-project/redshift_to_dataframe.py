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

def redshift_to_dataframe(data):
    df_labels = []

    for i in data['ColumnMetadata']:
        df_labels.append(i['label'])

    df_data = []

    for i in data['Records']:
        object_data = []

        for j in i:
            object_data.append(list(j.values())[0])

        df_data.append(object_data)

        df = pd.DataFrame(columns=df_labels, data=df_data)

        df.to_csv('your-file.csv')



def main():
  
    hook = PostgresHook(postgres_conn_id="my_redshift")
    conn = hook.get_conn()
    cur = conn.cursor()

    redshift_to_dataframe()

    conn.close()


if __name__ == "__main__":
    main()