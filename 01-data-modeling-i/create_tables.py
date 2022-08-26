from typing import NewType

import psycopg2


PostgresCursor = NewType("PostgresCursor", psycopg2.extensions.cursor)
PostgresConn = NewType("PostgresConn", psycopg2.extensions.connection)

table_drop_events = "DROP TABLE IF EXISTS Event"
table_drop_actors = "DROP TABLE IF EXISTS Actor"
table_drop_repos = "DROP TABLE IF EXISTS Repo"
table_drop_payloads = "DROP TABLE IF EXISTS Payload"
table_drop_orgs = "DROP TABLE IF EXISTS Org"
table_drop_issues = "DROP TABLE IF EXISTS Issue"
table_drop_releases = "DROP TABLE IF EXISTS Release"
table_drop_users = "DROP TABLE IF EXISTS Users"
table_drop_labels = "DROP TABLE IF EXISTS Label"
table_drop_commits = "DROP TABLE IF EXISTS Commits"
table_drop_mentions = "DROP TABLE IF EXISTS Mentions"

# Event
table_create_events = """
    CREATE TABLE IF NOT EXISTS Event (
        eventId INT NOT NULL,
        actorId INT NOT NULL,
        repoId INT NOT NULL,
        push_id INT NOT NULL,
        public BOOLEAN,
        created_at TIMESTAMP,
        orgId INT NOT NULL,
        PRIMARY KEY (eventId),
        CONSTRAINT fk_actor FOREIGN KEY (actorId)
            REFERENCES Actor (actorId),
        CONSTRAINT fk_repo FOREIGN KEY (repoId)
            REFERENCES Repo (repoId),
        CONSTRAINT fk_payload FOREIGN KEY (push_id)
            REFERENCES Payload (push_id)
            
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
        push_id INT NOT NULL,
        action VARCHAR(255),
        issueId INT NOT NULL,
        commentId INT NOT NULL,
        size INT,
        distinct_size INT,
        ref VARCHAR(255),
        head VARCHAR(255),
        before VARCHAR(255),
        commits VARCHAR(255),
        ref_type VARCHAR(255),
        master_branch VARCHAR(255),
        description VARCHAR(255),
        pusher_type VARCHAR(255),
        releaseId INT NOT NULL,
        PRIMARY KEY (push_id)
        
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

# Issue
table_create_issues = """
    CREATE TABLE IF NOT EXISTS Issue (
        issueId INT NOT NULL,
        url VARCHAR(255),
        repository_url VARCHAR(255),
        labels_url VARCHAR(255),
        comments_url VARCHAR(255),
        events_url VARCHAR(255),
        html_url VARCHAR(255),
        node_id VARCHAR(255),
        number INT,
        title VARCHAR(255),
        userId INT NOT NULL,
        labelId INT NOT NULL,
        state VARCHAR(255),
        locked BOOLEAN,
        assigneeId INT NOT NULL,
        assigneesId INT NOT NULL,
        milestone VARCHAR(255),
        comments VARCHAR(255),
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        closed_at TIMESTAMP,
        author_association VARCHAR(255),
        active_lock_reason VARCHAR(255),
        draft BOOLEAN,
        pull_request VARCHAR(255),
        body VARCHAR(255),
        reactions VARCHAR(255),
        timeline_url VARCHAR(255),
        performed_via_github_app VARCHAR(255),
        state_reason VARCHAR(255),
        open_issues INT,
        closed_issues INT,
        due_on VARCHAR(255),
        PRIMARY KEY (issueId)
        
            
    )
"""

# Release
table_create_releases = """
    CREATE TABLE IF NOT EXISTS Release (
        releaseId INT NOT NULL,
        url VARCHAR(255),
        assets_url VARCHAR(255),
        upload_url VARCHAR(255),
        html_url VARCHAR(255),
        authorId INT NOT NULL,
        node_id VARCHAR(255),
        tag_name VARCHAR(255),
        target_commitish VARCHAR(255),
        name VARCHAR(255),
        draft BOOLEAN,
        prerelease BOOLEAN,
        created_at TIMESTAMP ,
        published_at TIMESTAMP ,
        assets VARCHAR(255),
        tarball_url VARCHAR(255),
        zipball_url VARCHAR(255),
        body VARCHAR(255),
        mentions_count INT ,
        mentions VARCHAR(255),
        short_description_html VARCHAR(255),
        is_short_description_html_truncated BOOLEAN,
        PRIMARY KEY (releaseId)
        
    )
"""

# Users
table_create_users = """
    CREATE TABLE IF NOT EXISTS Users (
        userId INT NOT NULL,
        login VARCHAR(255),
        node_id VARCHAR(255),
        avatar_url VARCHAR(255),
        gravatar_id VARCHAR(255),
        url VARCHAR(255),
        html_url VARCHAR(255),
        followers_url VARCHAR(255),
        following_url VARCHAR(255),
        gists_url VARCHAR(255),
        starred_url VARCHAR(255),
        subscriptions_url VARCHAR(255),
        organizations_url VARCHAR(255),
        repos_url VARCHAR(255),
        events_url VARCHAR(255),
        received_events_url VARCHAR(255),
        type VARCHAR(255),
        site_admin BOOLEAN,
        PRIMARY KEY (userId)
    )
"""

# Label
table_create_labels = """
    CREATE TABLE IF NOT EXISTS Label (
        labelId INT NOT NULL,
        node_id VARCHAR(255),
        url VARCHAR(255),
        name VARCHAR(255),
        color VARCHAR(255),
        defaults BOOLEAN,
        description VARCHAR(255),
        PRIMARY KEY (labelId)
    )
"""

# Commits
table_create_commits = """
    CREATE TABLE IF NOT EXISTS Commits (
        sha VARCHAR(255),
        author_email VARCHAR(255),
        author_name VARCHAR(255),
        message VARCHAR(255),
        distincts BOOLEAN,
        url VARCHAR(255)
    )
"""

# Mentions
table_create_mentions = """
    CREATE TABLE IF NOT EXISTS Mentions (
        avatar_url VARCHAR(255),
        login VARCHAR(255),
        profile_name VARCHAR(255),
        profile_url VARCHAR(255),
        avatar_user_actor BOOLEAN
    )
"""

create_table_queries = [
    table_create_actors,
    table_create_repos,
    table_create_payloads,
    table_create_issues,
    table_create_releases,
    table_create_users,
    table_create_labels,

    table_create_events,
    table_create_orgs,
    table_create_commits,
    table_create_mentions,
]
drop_table_queries = [
    table_drop_events,
    table_drop_actors,
    table_drop_repos,
    table_drop_payloads,
    table_drop_orgs,
    table_drop_issues,
    table_drop_releases,
    table_drop_users,
    table_drop_labels,
    table_drop_commits,
    table_drop_mentions,
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


if __name__ == "__main__":
    main()