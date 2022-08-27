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

    i = 0
    for datafile in all_files:
        with open(datafile, "rb") as f:
            data = json.loads(f.read())
            for each in data:
                
                # Insert data into tables here
                
                insert_statement_actor = f"""
                    INSERT INTO Actor (actorId,login,display_login,gravatar_id,url,avatar_url)
                    VALUES ('{each["actor"]["id"]}'
                            , '{each["actor"]["login"]}'
                            , '{each["actor"]["display_login"]}'
                            , '{each["actor"]["gravatar_id"]}'
                            , '{each["actor"]["url"]}'
                            , '{each["actor"]["avatar_url"]}')
                    ON CONFLICT (actorId) DO NOTHING
                """
                cur.execute(insert_statement_actor)

                insert_statement_repo = f"""
                    INSERT INTO Repo (repoId,name,url)
                    VALUES ('{each["repo"]["id"]}'
                            , '{each["repo"]["name"]}'
                            , '{each["repo"]["url"]}')
                    ON CONFLICT (repoId) DO NOTHING
                """
                cur.execute(insert_statement_repo)

                try:
                    insert_statement_org = f"""
                        INSERT INTO Org (orgId,login,gravatar_id,url,avatar_url)
                        VALUES ('{each["org"]["id"]}'
                                , '{each["org"]["login"]}'
                                , '{each["org"]["gravatar_id"]}'
                                , '{each["org"]["url"]}'
                                , '{each["org"]["avatar_url"]}')
                        ON CONFLICT (orgId) DO NOTHING
                    """
                    cur.execute(insert_statement_org)
                except:
                    pass

                try:
                    insert_statement_event = f"""
                        INSERT INTO Event (eventId,type,actorId,repoId,public,created_at,orgId) 
                        VALUES ('{each["id"]}'
                                , '{each["type"]}'
                                , '{each["actor"]["id"]}'
                                , '{each["repo"]["id"]}'
                                , '{each["public"]}'
                                , '{each["created_at"]}'
                                , '{each["org"]["id"]}'
                                )
                        ON CONFLICT (eventId) DO NOTHING
                    """
                    cur.execute(insert_statement_event)
                except:
                    pass

               
                try:
                    conn.commit()
                    i = i+1

                except:
                    # Rolling back in case of error
                    conn.rollback()
    
    print("Data inserted total " + str(i) + " records")


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    process(cur, conn, filepath="../data")

    conn.close()


if __name__ == "__main__":
    main()