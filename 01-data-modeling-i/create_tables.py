import psycopg2


table_drop = "DROP TABLE IF EXISTS events;DROP TABLE IF EXISTS actor;DROP TABLE IF EXISTS org;DROP TABLE IF EXISTS repo"

table_create = """
CREATE TABLE IF NOT EXISTS actor (
                actor_id INTEGER PRIMARY KEY,
                login VARCHAR(50) ,
                display_login VARCHAR(100) ,
                gravatar_id VARCHAR(100),
                url VARCHAR(255) ,
                avatar_url VARCHAR(255) 
);
CREATE TABLE IF NOT EXISTS org (
                org_id INTEGER PRIMARY KEY,
                login VARCHAR(50) ,
                gravatar_id VARCHAR(100),
                url VARCHAR(255) ,
                avatar_url VARCHAR(255) 

);
CREATE TABLE IF NOT EXISTS repo (
                repo_id INTEGER PRIMARY KEY,
                name VARCHAR(100) ,
                url VARCHAR(255) 
);
CREATE TABLE IF NOT EXISTS events (
                events_id BIGINT PRIMARY KEY,
                type VARCHAR ,
                public VARCHAR ,
                created_at TIMESTAMP ,
                actor_id INTEGER ,
                org_id INTEGER ,
                repo_id INTEGER ,
                CONSTRAINT fk_actor FOREIGN KEY(actor_id) REFERENCES actor(actor_id) ,
                CONSTRAINT fk_org FOREIGN KEY(org_id) REFERENCES org(org_id) ,
                CONSTRAINT fk_repo FOREIGN KEY(repo_id) REFERENCES repo(repo_id)
)
"""


create_table_queries = [
    table_create
]
drop_table_queries = [
    table_drop
]


def drop_tables(cur, conn) -> None:
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn) -> None:
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