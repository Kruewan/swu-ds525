import psycopg2


drop_table_queries = [
    "DROP TABLE IF EXISTS events",
    "DROP TABLE IF EXISTS actors",
    "DROP TABLE IF EXISTS repos",
    "DROP TABLE IF EXISTS payloads",
    "DROP TABLE IF EXISTS orgs",
]
create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS events (
        eventId text,
        type text,
        actor text,
        repo text,
        action text,
        public BOOLEAN,
        created_at text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS actors (
        actorId int,
        login text,
        display_login text,
        gravatar_id text,
        url text,
        avatar_url text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS repos (
        repoId int,
        name text,
        url text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS payloads (
        push_id int,
        size int,
        distinct_size int,
        ref text,
        head text,
        before text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS orgs (
        orgId int,
        login text,
        gravatar_id text,
        url text,
        avatar_url text
    )
    """,
]
copy_table_queries = [
    """
    COPY staging_events FROM 's3://zkan-swu-labs/github_events_01.json'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::377290081649:role/LabRole'
    JSON 's3://zkan-swu-labs/events_json_path.json'
    REGION 'us-east-1'
    """,
]
insert_table_queries = [
    """
    INSERT INTO
      events (
        id
      )
    SELECT
      DISTINCT id,
    FROM
      staging_events
    WHERE
      id NOT IN (SELECT DISTINCT id FROM events)
    """,
]


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    host = "redshift-cluster-1.cdagfejibaqn.us-east-1.redshift.amazonaws.com"
    dbname = "dev"
    user = "awsuser"
    password = "hT51cr6y"
    port = "5439"
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    load_tables(cur, conn)
    insert_tables(cur, conn)

    query = "select * from category"
    cur.execute(query)

    records = cur.fetchall()
    for row in records:
        print(row)

    conn.close()


if __name__ == "__main__":
    main()
