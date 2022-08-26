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
        with open(datafile, "rb") as f:
            data = json.loads(f.read())
            for each in data:
                
                # Insert data into tables here
                insert_statement_event = f"""
                    INSERT INTO Event (eventId,actorId,repoId ,public ,orgId) 
                    VALUES ({each["id"]}
                            , {each["actor"]["id"]}
                            , {each["repo"]["id"]}
                            
                            , {each["public"]}
                            
                            , {each["org"]["id"]})
                    ON CONFLICT (eventId) DO NOTHING
                """
                cur.execute(insert_statement_event)

                insert_statement_actor = f"""
                    INSERT INTO Actor (actorId,login,display_login,gravatar_id,url,avatar_url)
                    VALUES ({each["actor"]["id"]}
                            , {each["actor"]["login"]}
                            , {each["actor"]["display_login"]}
                            , {each["actor"]["gravatar_id"]}
                            , {each["actor"]["url"]}
                            , {each["actor"]["avatar_url"]})
                    ON CONFLICT (actorId) DO NOTHING
                """
                cur.execute(insert_statement_actor)

                insert_statement_repo = f"""
                    INSERT INTO Repo (repoId,name,url)
                    VALUES ({each["repo"]["id"]}
                            , {each["repo"]["name"]}
                            , {each["repo"]["url"]})
                    ON CONFLICT (repoId) DO NOTHING
                """
                cur.execute(insert_statement_repo)

                insert_statement_payload = f"""
                    INSERT INTO Payload (push_id,action,issueId,commentId,size,distinct_size,
                        ref,head,before,commits,ref_type,master_branch,
                        description,pusher_type,releaseId)
                    VALUES ({each["payload"]["push_id"]}
                            , {each["payload"]["action"]}
                            , {each["payload"]["issue"]["id"]}
                            , {each["payload"]["comment"]["id"]}
                            , {each["payload"]["size"]}
                            , {each["payload"]["distinct_size"]}
                            , {each["payload"]["ref"]}
                            , {each["payload"]["head"]}
                            , {each["payload"]["before"]}
                            , {each["payload"]["commits"]}
                            , {each["payload"]["ref_type"]}
                            , {each["payload"]["master_branch"]}
                            , {each["payload"]["description"]}
                            , {each["payload"]["pusher_type"]}
                            , {each["payload"]["release"]["id"]})
                    ON CONFLICT (push_id) DO NOTHING
                """
                cur.execute(insert_statement_payload)

                insert_statement_org = f"""
                    INSERT INTO Org (orgId,login,gravatar_id,url,avatar_url)
                    VALUES ({each["org"]["id"]}
                            , {each["org"]["login"]}
                            , {each["org"]["gravatar_id"]}
                            , {each["org"]["url"]}
                            , {each["org"]["avatar_url"]})
                    ON CONFLICT (orgId) DO NOTHING
                """
                cur.execute(insert_statement_org)

                insert_statement_issue = f"""
                    INSERT INTO Issue (issueId,url,repository_url,labels_url,comments_url,
                        events_url,html_url,node_id,number,title,userId,
                        labelId,state,locked,assigneeId,assigneesId,milestone,
                        comments,created_at,updated_at,closed_at,author_association,
                        active_lock_reason,draft,pull_request,body,reactions,
                        timeline_url,performed_via_github_app,state_reason,
                        open_issues,closed_issues,due_on) 
                    VALUES ({each["payload"]["issue"]["id"]}
                            , {each["payload"]["issue"]["url"]}
                            , {each["payload"]["issue"]["repository_url"]}
                            , {each["payload"]["issue"]["labels_url"]}
                            , {each["payload"]["issue"]["comments_url"]}
                            , {each["payload"]["issue"]["events_url"]}
                            , {each["payload"]["issue"]["html_url"]}
                            , {each["payload"]["issue"]["node_id"]}
                            , {each["payload"]["issue"]["number"]}
                            , {each["payload"]["issue"]["title"]}
                            , {each["payload"]["issue"]["user"]["id"]}
                            , {each["payload"]["issue"]["labels"]["id"]}
                            , {each["payload"]["issue"]["state"]}
                            , {each["payload"]["issue"]["locked"]}
                            , {each["payload"]["issue"]["assignee"]["id"]}
                            , {each["payload"]["issue"]["assignees"]["id"]}
                            , {each["payload"]["issue"]["milestone"]}
                            , {each["payload"]["issue"]["comments"]}
                            , {each["payload"]["issue"]["created_at"]}
                            , {each["payload"]["issue"]["updated_at"]}
                            , {each["payload"]["issue"]["closed_at"]}
                            , {each["payload"]["issue"]["author_association"]}
                            , {each["payload"]["issue"]["active_lock_reason"]}
                            , {each["payload"]["issue"]["draft"]}
                            , {each["payload"]["issue"]["pull_request"]["url"]}
                            , {each["payload"]["issue"]["body"]}
                            , {each["payload"]["issue"]["reactions"]["url"]}
                            , {each["payload"]["issue"]["timeline_url"]}
                            , {each["payload"]["issue"]["performed_via_github_app"]}
                            , {each["payload"]["issue"]["state_reason"]}
                            , {each["payload"]["issue"]["milestone"]["open_issues"]}
                            , {each["payload"]["issue"]["milestone"]["closed_issues"]}
                            , {each["payload"]["issue"]["milestone"]["due_on"]})
                    ON CONFLICT (issueId) DO NOTHING
                """
                cur.execute(insert_statement_issue)

                insert_statement_release = f"""
                    INSERT INTO Release (releaseId,url,assets_url,upload_url,html_url,authorId,node_id,
                        tag_name,target_commitish,name,draft,prerelease,created_at,
                        published_at,assets,tarball_url,zipball_url,body,mentions_count,
                        mentions,short_description_html,is_short_description_html_truncated)
                    VALUES ({each["payload"]["release"]["id"]}
                            , {each["payload"]["release"]["url"]}
                            , {each["payload"]["release"]["assets_url"]}
                            , {each["payload"]["release"]["upload_url"]}
                            , {each["payload"]["release"]["html_url"]}
                            , {each["payload"]["release"]["author"]["id"]}
                            , {each["payload"]["release"]["node_id"]}
                            , {each["payload"]["release"]["tag_name"]}
                            , {each["payload"]["release"]["target_commitish"]}
                            , {each["payload"]["release"]["name"]}
                            , {each["payload"]["release"]["draft"]}
                            , {each["payload"]["release"]["prerelease"]}
                            , {each["payload"]["release"]["created_at"]}
                            , {each["payload"]["release"]["published_at"]}
                            , {each["payload"]["release"]["assets"]}
                            , {each["payload"]["release"]["tarball_url"]}
                            , {each["payload"]["release"]["zipball_url"]}
                            , {each["payload"]["release"]["body"]}
                            , {each["payload"]["release"]["mentions_count"]}
                            , {each["payload"]["release"]["mentions"]["avatar_url"]}
                            , {each["payload"]["release"]["short_description_html"]}
                            , {each["payload"]["release"]["is_short_description_html_truncated"]})
                    ON CONFLICT (releaseId) DO NOTHING
                """
                cur.execute(insert_statement_release)

                insert_statement_users = f"""
                    INSERT INTO Users (userId,login,node_id,avatar_url,gravatar_id,url,html_url,
                        followers_url,following_url,gists_url,starred_url,subscriptions_url,
                        organizations_url,repos_url,events_url,received_events_url,type,
                        site_admin)
                    VALUES ({each["payload"]["issue"]["user"]["id"]}
                            , {each["payload"]["issue"]["user"]["login"]}
                            , {each["payload"]["issue"]["user"]["node_id"]}
                            , {each["payload"]["issue"]["user"]["avatar_url"]}
                            , {each["payload"]["issue"]["user"]["gravatar_id"]}
                            , {each["payload"]["issue"]["user"]["url"]}
                            , {each["payload"]["issue"]["user"]["html_url"]}
                            , {each["payload"]["issue"]["user"]["followers_url"]}
                            , {each["payload"]["issue"]["user"]["following_url"]}
                            , {each["payload"]["issue"]["user"]["gists_url"]}
                            , {each["payload"]["issue"]["user"]["user"]["starred_url"]}
                            , {each["payload"]["issue"]["user"]["subscriptions_url"]}
                            , {each["payload"]["issue"]["user"]["organizations_url"]}
                            , {each["payload"]["issue"]["user"]["repos_url"]}
                            , {each["payload"]["issue"]["user"]["events_url"]}
                            , {each["payload"]["issue"]["user"]["received_events_url"]}
                            , {each["payload"]["issue"]["user"]["type"]}
                            , {each["payload"]["issue"]["user"]["site_admin"]})
                    ON CONFLICT (userId) DO NOTHING
                """
                cur.execute(insert_statement_users)

                insert_statement_label = f"""
                    INSERT INTO Label (labelId,node_id,url,name,color,default,description)
                    VALUES ({each["payload"]["issue"]["labels"]["id"]}
                            , {each["payload"]["issue"]["labels"]["node_id"]}
                            , {each["payload"]["issue"]["labels"]["url"]}
                            , {each["payload"]["issue"]["labels"]["name"]}
                            , {each["payload"]["issue"]["labels"]["color"]}
                            , {each["payload"]["issue"]["labels"]["default"]}
                            , {each["payload"]["issue"]["labels"]["description"]})
                    ON CONFLICT (labelId) DO NOTHING
                """
                cur.execute(insert_statement_label)

                insert_statement_commits = f"""
                    INSERT INTO Commits (sha,author_email,author_name,message,distinct,url)
                    VALUES ({each["payload"]["commits"]["sha"]}
                            , {each["payload"]["commits"]["author_email"]}
                            , {each["payload"]["commits"]["author_name"]}
                            , {each["payload"]["commits"]["message"]}
                            , {each["payload"]["commits"]["distinct"]}
                            , {each["payload"]["commits"]["url"]})
                    
                """
                cur.execute(insert_statement_commits)

                insert_statement_mentions = f"""
                    INSERT INTO Mentions (avatar_url,login,profile_name,profile_url,avatar_user_actor)
                    VALUES ({each["payload"]["release"]["mentions"]["avatar_url"]}
                            , {each["payload"]["release"]["mentions"]["login"]}
                            , {each["payload"]["release"]["mentions"]["profile_name"]}
                            , {each["payload"]["release"]["mentions"]["profile_url"]}
                            , {each["payload"]["release"]["mentions"]["avatar_user_actor"]})
                    
                """
                cur.execute(insert_statement_mentions)
                
                try:
                    conn.commit()

                except:
                    # Rolling back in case of error
                    conn.rollback()
                    print("Data inserted")


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    process(cur, conn, filepath="../data")

    conn.close()


if __name__ == "__main__":
    main()