from cassandra.cluster import Cluster
import glob
import json
import os
from typing import List


table_drop_total_events = "DROP TABLE IF EXISTS total_events"
table_drop_users = "DROP TABLE IF EXISTS users"


table_create_total_events = """
    CREATE TABLE IF NOT EXISTS total_events
    (
        type text,
        actor_login text,
        actor_display_login text,
        PRIMARY KEY (
            actor_login,
            type
        )
    )
"""

table_create_users = """
    CREATE TABLE IF NOT EXISTS users
    (
        user text,
        total int ,
        PRIMARY KEY (
            total,
            user
        )
    )
"""

create_table_queries = [
    table_create_total_events,
    table_create_users,
]
drop_table_queries = [
    table_drop_users,
    table_drop_total_events,
]

def drop_tables(session):
    for query in drop_table_queries:
        try:
            rows = session.execute(query)
        except Exception as e:
            print(e)


def create_tables(session):
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print(e)

def get_files(filepath: str) -> List[str]:
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

def process(session, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    # Insert data for UI
    j = 0
    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:

                try:
                    # Insert data into tables total_events
                    query = f"""INSERT INTO total_events (type, actor_login, actor_display_login) VALUES ('{each["type"]}', '{each["actor"]["login"]}', '{each["actor"]["display_login"]}')"""
                    session.execute(query)
                    j = j+1
                except:
                    pass

    print("Data inserted total " + str(j) + " records")


def insert_data(session):

    query_select = """
    SELECT actor_login, COUNT (*)  AS user_count  FROM  total_events  GROUP BY  actor_login  -- limit 10
    """
    try:
        rows = session.execute(query_select)
    except Exception as e:
        print(e)

    for row in rows:
        try:
            query_insert = f"""
            INSERT INTO users (user, total) VALUES ('{row[0]}', {row[1]})
            """
            session.execute(query_insert)
        except Exception as e:
            print(e)


def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

    # Create keyspace
    try:
        session.execute(
            """
            CREATE KEYSPACE IF NOT EXISTS github_events
            WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
            """
        )
    except Exception as e:
        print(e)

    # Set keyspace
    try:
        session.set_keyspace("github_events")
    except Exception as e:
        print(e)

    drop_tables(session)
    create_tables(session)

    process(session, filepath="../data")
   
    insert_data(session)

    # Select data in Cassandra and print them to stdout
    query = """
    SELECT  user, total  from users  
    """

    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)


    i = 0
    ls_data = list(rows)
    for row in reversed(ls_data):
        if (i < 10):
            print(row)
            i = i+1

    


if __name__ == "__main__":
    main()