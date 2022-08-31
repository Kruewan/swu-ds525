from cassandra.cluster import Cluster
import glob
import json
import os
from typing import List

table_drop_events = "DROP TABLE IF EXISTS events"
table_drop_payloads = "DROP TABLE IF EXISTS payloads"
table_drop_actors = "DROP TABLE IF EXISTS actors"

table_create_events = """
    CREATE TABLE IF NOT EXISTS events
    (
        id text,
        type text,
        public text,
        PRIMARY KEY (
            id,
            type
        )
    )
"""

table_create_actors = """
    CREATE TABLE IF NOT EXISTS actors
    (
        actor_id text,
        actor_login text,
        actor_display_login text,
        PRIMARY KEY (
            actor_id,
            actor_display_login
        )
    )
"""

table_create_payloads = """
    CREATE TABLE IF NOT EXISTS payloads
    (
        id text,
        action text,
        PRIMARY KEY (
            id
        )
    )
"""

create_table_queries = [
    table_create_events,
    table_create_payloads,
    table_create_actors,
]
drop_table_queries = [
    table_drop_events,
    table_drop_payloads,
    table_drop_actors,
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

    i = 0
    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:

                try:
                    # Insert data into tables events
                    query = f"""INSERT INTO events (id, type, action, public) VALUES ('{each["id"]}', '{each["type"]}', '{each["payload"]["action"]}', '{each["public"]}')"""
                    session.execute(query)
                    i = i+1
                except:
                    pass

                try:
                    # Insert data into tables actors
                    query = f"""INSERT INTO actors (actor_id,actor_login,actor_display_login) VALUES ('{each["actor"]["id"]}', '{each["actor"]["login"]}', '{each["actor"]["display_login"]}')"""
                    session.execute(query)
                except:
                    pass

                try:
                    # Insert data into tables payloads
                    query = f"""INSERT INTO payloads (id, action) VALUES ('{each["id"]}', '{each["payload"]["action"]}')"""
                    session.execute(query)
                except:
                    pass


    print("Data inserted total " + str(i) + " records")



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
   
    # Select data in Cassandra and print them to stdout
    query = """
    SELECT actor_id, actor_display_login, COUNT (*)  AS user_count  FROM actors  GROUP BY actor_id, actor_display_login
    """
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()