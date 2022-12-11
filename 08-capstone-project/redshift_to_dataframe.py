from typing import NewType

import psycopg2


PostgresCursor = NewType("PostgresCursor", psycopg2.extensions.cursor)
PostgresConn = NewType("PostgresConn", psycopg2.extensions.connection)

table_drop_events = "DROP TABLE IF EXISTS Event"
table_drop_actors = "DROP TABLE IF EXISTS Actor"
table_drop_repos = "DROP TABLE IF EXISTS Repo"
table_drop_payloads = "DROP TABLE IF EXISTS Payload"
table_drop_orgs = "DROP TABLE IF EXISTS Org"

# Event
table_create_events = """
    CREATE TABLE IF NOT EXISTS Event (
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
            REFERENCES Actor (actorId),
        CONSTRAINT fk_repo FOREIGN KEY (repoId)
            REFERENCES Repo (repoId)
    )
"""

# Actor
table_create_actors = """
    CREATE TABLE IF NOT EXISTS Actor (
        actorId INT NOT NULL,
        login VARCHAR(255),
        display_login VARCHAR(255),
        gravatar_id VARCHAR(255),
        url VARCHAR(255),
        avatar_url VARCHAR(255),
        PRIMARY KEY (actorId)
    )
"""

# Repo
table_create_repos = """
    CREATE TABLE IF NOT EXISTS Repo (
        repoId INT NOT NULL,
        name VARCHAR(255),
        url VARCHAR(255),
        PRIMARY KEY (repoId)
    )
"""

# Payload
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

# Org
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
drop_table_queries = [
    table_drop_events,
    table_drop_actors,
    table_drop_repos,
    table_drop_payloads,
    table_drop_orgs,
]


def drop_tables(cur: PostgresCursor, conn: PostgresConn) -> None:
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur: PostgresCursor, conn: PostgresConn) -> None:
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database.
    - Establishes connection with the sparkify database and gets
    cursor to it.
    - Drops all the tables.
    - Creates all tables needed.
    - Finally, closes the connection.
    """
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

    print("Table created")


if __name__ == "__main__":
    main()