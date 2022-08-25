import psycopg2


table_drop = "DROP TABLE IF EXISTS songplays"

table_create = """
    CREATE TABLE IF NOT EXISTS Actor 
    (
        id INT AUTO_INCREMENT PRIMARY KEY, 
        login VARCHAR(255),
        display_login VARCHAR(255),
        gravatar_id VARCHAR(255),
        url VARCHAR(255),
        avatar_url VARCHAR(255)
    ),

    CREATE TABLE IF NOT EXISTS Author
    (
        email VARCHAR(255),
        name VARCHAR(255),
        login VARCHAR(255),
        public int id VARCHAR(255),
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
        public bool site_admin VARCHAR(255)
    ),

    CREATE TABLE IF NOT EXISTS Comment
    (
        url VARCHAR(255),
        html_url VARCHAR(255),
        issue_url VARCHAR(255),
        public int id VARCHAR(255),
        node_id VARCHAR(255),
        public User user VARCHAR(255),
        public DateTime created_at VARCHAR(255),
        public DateTime updated_at VARCHAR(255),
        author_association VARCHAR(255),
        body VARCHAR(255),
        public Reactions reactions VARCHAR(255),
        public object performed_via_github_app VARCHAR(255)
    ),

    CREATE TABLE IF NOT EXISTS Commit
    (
        sha VARCHAR(255),
        public Author author VARCHAR(255),
        message VARCHAR(255),
        public bool distinct VARCHAR(255),
        url VARCHAR(255)
    ),

    CREATE TABLE IF NOT EXISTS Issue
    (
        url VARCHAR(255),
        repository_url VARCHAR(255),
        labels_url VARCHAR(255),
        comments_url VARCHAR(255),
        events_url VARCHAR(255),
        html_url VARCHAR(255),
        public int id VARCHAR(255),
        node_id VARCHAR(255),
        public int number VARCHAR(255),
        title VARCHAR(255),
        public User user VARCHAR(255),
        public List<Label> labels VARCHAR(255),
        state VARCHAR(255),
        public bool locked VARCHAR(255),
        public Assignee assignee VARCHAR(255),
        public List<Assignee> assignees VARCHAR(255),
        public Milestone milestone VARCHAR(255),
        public int comments VARCHAR(255),
        public DateTime created_at VARCHAR(255),
        public DateTime updated_at VARCHAR(255),
        public DateTime? closed_at VARCHAR(255),
        author_association VARCHAR(255),
        public object active_lock_reason VARCHAR(255),
        body VARCHAR(255),
        public Reactions reactions VARCHAR(255),
        timeline_url VARCHAR(255),
        public object performed_via_github_app VARCHAR(255),
        state_reason VARCHAR(255),
        public bool? draft VARCHAR(255),
        public PullRequest pull_request VARCHAR(255)
    ),

    CREATE TABLE IF NOT EXISTS Label
    (
        public object id VARCHAR(255),
        node_id VARCHAR(255),
        url VARCHAR(255),
        name VARCHAR(255),
        color VARCHAR(255),
        public bool @default VARCHAR(255),
        description VARCHAR(255)
    ),

    CREATE TABLE IF NOT EXISTS Mention
    (
        avatar_url VARCHAR(255),
        login VARCHAR(255),
        profile_name VARCHAR(255),
        profile_url VARCHAR(255),
        public bool avatar_user_actor VARCHAR(255)
    ),

    CREATE TABLE IF NOT EXISTS Milestone
    (
        url VARCHAR(255),
        html_url VARCHAR(255),
        labels_url VARCHAR(255),
        public int id VARCHAR(255),
        node_id VARCHAR(255),
        public int number VARCHAR(255),
        title VARCHAR(255),
        description VARCHAR(255),
        public Creator creator VARCHAR(255),
        public int open_issues VARCHAR(255),
        public int closed_issues VARCHAR(255),
        state VARCHAR(255),
        public DateTime created_at VARCHAR(255),
        public DateTime updated_at VARCHAR(255),
        public object due_on VARCHAR(255),
        public object closed_at VARCHAR(255)
    ),

    CREATE TABLE IF NOT EXISTS Org
    (
        public int id VARCHAR(255),
        login VARCHAR(255),
        gravatar_id VARCHAR(255),
        url VARCHAR(255),
        avatar_url VARCHAR(255)
    ),

    CREATE TABLE IF NOT EXISTS Payload
    (
        action VARCHAR(255),
        public Issue issue VARCHAR(255),
        public Comment comment VARCHAR(255),
        public long? push_id VARCHAR(255),
        public int? size VARCHAR(255),
        public int? distinct_size VARCHAR(255),
        @ref VARCHAR(255),
        head VARCHAR(255),
        before VARCHAR(255),
        public List<Commit> commits VARCHAR(255),
        ref_type VARCHAR(255),
        master_branch VARCHAR(255),
        description VARCHAR(255),
        pusher_type VARCHAR(255),
        public Release release VARCHAR(255)
    ),

    CREATE TABLE IF NOT EXISTS PullRequest
    (
        url VARCHAR(255),
        html_url VARCHAR(255),
        diff_url VARCHAR(255),
        patch_url VARCHAR(255),
        public object merged_at VARCHAR(255)
    ),

    CREATE TABLE IF NOT EXISTS Reactions
    (
        url VARCHAR(255),
        public int total_count VARCHAR(255),

        public int laugh VARCHAR(255),
        public int hooray VARCHAR(255),
        public int confused VARCHAR(255),
        public int heart VARCHAR(255),
        public int rocket VARCHAR(255),
        public int eyes VARCHAR(255)
    ),

    CREATE TABLE IF NOT EXISTS Release
    (
        url VARCHAR(255),
        assets_url VARCHAR(255),
        upload_url VARCHAR(255),
        html_url VARCHAR(255),
        public int id VARCHAR(255),
        public Author author VARCHAR(255),
        node_id VARCHAR(255),
        tag_name VARCHAR(255),
        target_commitish VARCHAR(255),
        name VARCHAR(255),
        public bool draft VARCHAR(255),
        public bool prerelease VARCHAR(255),
        public DateTime created_at VARCHAR(255),
        public DateTime published_at VARCHAR(255),
        public List<object> assets VARCHAR(255),
        tarball_url VARCHAR(255),
        zipball_url VARCHAR(255),
        body VARCHAR(255),
        public int mentions_count VARCHAR(255),
        public List<Mention> mentions VARCHAR(255),
        short_description_html VARCHAR(255),
        public bool is_short_description_html_truncated VARCHAR(255)
    ),

    CREATE TABLE IF NOT EXISTS Repo
    (
        public int id VARCHAR(255),
        name VARCHAR(255),
        url VARCHAR(255)
    ),

    CREATE TABLE IF NOT EXISTS Root
    (
        id VARCHAR(255),
        type VARCHAR(255),
        public Actor actor VARCHAR(255),
        public Repo repo VARCHAR(255),
        public Payload payload VARCHAR(255),
        public bool @public VARCHAR(255),
        public DateTime created_at VARCHAR(255),
        public Org org VARCHAR(255)
    )

"""

create_table_queries = [
    table_create,
]
drop_table_queries = [
    table_drop,
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
