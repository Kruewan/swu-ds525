from cassandra.cluster import Cluster
import glob
import json
import os
from typing import List

table_drop_events = "DROP TABLE IF EXISTS Event"
table_drop_payloads = "DROP TABLE IF EXISTS Payload"

# Event
table_create_events = """
    CREATE TABLE IF NOT EXISTS Event (
        eventId TEXT NOT NULL,
        type VARCHAR(255),
        action VARCHAR(255),
        public BOOLEAN,
        created_at TIMESTAMP,
        PRIMARY KEY (eventId)
    )
"""

# Payload
table_create_payloads = """
    CREATE TABLE IF NOT EXISTS Payload (
        eventId TEXT NOT NULL,
        action VARCHAR(255),
        userId VARCHAR(255),
        userLogin VARCHAR(255),
        PRIMARY KEY (eventId)
    )
"""

create_table_queries = [
    table_create_payloads,
    table_create_events,
]
drop_table_queries = [
    table_drop_events,
    table_drop_payloads,
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

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data
                print(each["id"], each["type"], each["actor"]["login"])

                # Insert data into tables here
                
                # Insert table event
                try:
                    insert_statement_event = f"""
                        INSERT INTO Event (eventId,type,action,public,created_at) 
                        VALUES ('{each["id"]}'
                                , '{each["type"]}'
                                , '{each["payload"]["action"]}'
                                , '{each["public"]}'
                                , '{each["created_at"]}'
                                )
                        ON CONFLICT (eventId) DO NOTHING
                    """
                    session.execute(insert_statement_event)
                except:
                    pass

                # Insert table Payload
                try:
                    insert_statement_payload = f"""
                        INSERT INTO Payload (eventId,action,userId,userLogin) 
                        VALUES ('{each["id"]}'
                                , '{each["type"]}'
                                , '{each["payload"]["action"]}'
                                , '{each["public"]}'
                                , '{each["created_at"]}'
                                )
                        ON CONFLICT (eventId) DO NOTHING
                    """
                    session.execute(insert_statement_payload)
                except:
                    pass


                try:
                    session.commit()
                    i = i+1

                except:
                    # Rolling back in case of error
                    session.rollback()
    
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
    SELECT * from events WHERE id = '23487929637' AND type = 'IssueCommentEvent'
    """
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()