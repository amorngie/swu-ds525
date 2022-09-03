import glob
import json
import os
from typing import List

import psycopg2


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


def process(cur, conn, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
                # Print some sample data

                
                
                if each["type"] == "IssueCommentEvent":
                    print(
                        each["id"], 
                        each["type"],
                        each["actor"]["id"],
                        each["actor"]["login"],
                        each["repo"]["id"],
                        each["repo"]["name"],
                        each["created_at"]

                    )

                else:
                    print(
                        each["id"], 
                        each["type"],
                        each["actor"]["id"],
                        each["actor"]["login"],
                        each["repo"]["id"],
                        each["repo"]["name"],
                        each["created_at"]
                    )
                    

                
                # Insert data into tables here
                insert_statement = f"""
                    INSERT INTO actor (
                        actor_id,
                        login,
                        display_login,
                        gravatar_id,
                        url,
                        avatar_url
                    ) VALUES ({each["actor"]["id"]}, '{each["actor"]["login"]}','{each["actor"]["display_login"]}','{each["actor"]["gravatar_id"]}','{each["actor"]["url"]}','{each["actor"]["avatar_url"]}')

                    ON CONFLICT (actor_id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_statement)

                # Insert data into tables here
                insert_statement = f"""
                    INSERT INTO repo (
                        repo_id,
                        name,
                        url
                    ) VALUES ({each["repo"]["id"]}, '{each["repo"]["name"]}','{each["repo"]["url"]}')

                    ON CONFLICT (repo_id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_statement)
                
                try:
                    # Insert data into tables here
                    insert_statement = f"""
                        INSERT INTO org (
                            org_id,
                            login,
                            gravatar_id,
                            url,
                            avatar_url
                        ) VALUES ({each["org"]["id"]}, '{each["org"]["login"]}','{each["org"]["gravatar_id"]}','{each["org"]["url"]}','{each["org"]["avatar_url"]}')

                        ON CONFLICT (org_id) DO NOTHING
                    """
                    # print(insert_statement)
                    cur.execute(insert_statement)
                except:
                    pass

                try:
                    # Insert data into tables here
                    insert_statement = f"""
                        INSERT INTO events (
                            events_id,
                            type,
                            public,
                            created_at,
                            actor_id,
                            org_id,
                            repo_id
                        ) VALUES ({each["id"]},'{each["type"]}','{each["public"]}','{each["created_at"]}', '{each["actor"]["id"]}','{each["org"]["id"]}','{each["repo"]["id"]}')
                        ON CONFLICT (events_id) DO NOTHING
                        """
                    # print(insert_statement)
                    cur.execute(insert_statement)
                except:
                    pass    

                conn.commit()


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    process(cur, conn, filepath="../data")

    conn.close()


if __name__ == "__main__":
    main()