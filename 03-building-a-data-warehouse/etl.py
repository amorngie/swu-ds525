import psycopg2


drop_table_queries = [
    "DROP TABLE IF EXISTS events",
    "DROP TABLE IF EXISTS actors",
    "DROP TABLE IF EXISTS repos",
    "DROP TABLE IF EXISTS staging_events",
]
create_table_queries = [
    """
    CREATE TABLE IF NOT EXISTS staging_events (
        id text,
        type text,
        actor text,
        public text,
        created_at text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        id text,
        type text,
        actor text,
        public text,
        created_at text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS actors (
        id text,
        actor text,
        display_login text,
        url text
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS repos (
        repo_id int,
        name text,
        url text
    )
    """


]
copy_table_queries = [
    """
    COPY staging_events FROM 's3://amk-ds525/github_events_01.json'
    CREDENTIALS 'aws_iam_role=arn:aws:iam::043167588229:role/LabRole'
    JSON 's3://amk-ds525/events_json_path.json'
    REGION 'us-east-1'
    """,
]
insert_table_queries = [
    """
    INSERT INTO
      staging_events (
        id,
        type,
        actor,
        public,
        created_at
      )
    SELECT
        id, type,actor, public, created_at
    FROM
      events
    WHERE
      type NOT IN (SELECT DISTINCT type FROM staging_events)
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


def load_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    host = "redshift-cluster-1.cgpttgasm2sh.us-east-1.redshift.amazonaws.com"
    dbname = "dev"
    user = "awsuser"
    password = "GeeGiiEz12"
    port = "5439"
    conn_str = f"host={host} dbname={dbname} user={user} password={password} port={port}"
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    load_tables(cur, conn)
    insert_tables(cur, conn)

    query = "select * from events"
    cur.execute(query)
    records = cur.fetchall()
    for row in records:
        print(row)

    conn.close()


if __name__ == "__main__":
    main()