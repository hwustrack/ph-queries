import json
import sys
import time
import os.path
from os import path

from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport


CACHE_FILE_NAME = "posts.json"
CACHE_CURSOR_FILE_NAME = "posts.cursor.json"

def get_posts(auth_token, use_cache, update_cache):
    if (use_cache):
        with open(CACHE_FILE_NAME, 'r') as f:
            posts = json.load(f)
    else:
        (posts, page_info) = get_posts_ph(auth_token)

    if not use_cache and update_cache:
        with open(CACHE_FILE_NAME, 'w') as f:
            json.dump(posts, f, indent=4)
        
        with open(CACHE_CURSOR_FILE_NAME, 'w') as f:
            json.dump(page_info, f, indent=4)

    return posts


def get_posts_ph(auth_token):
    client = Client(
        transport=RequestsHTTPTransport(
            url='https://api.producthunt.com/v2/api/graphql',
            headers={'Authorization': 'Bearer ' + auth_token}
        )
    )

    MAX_REQUESTS = 450
    MAX_POSTS = 200
    SLEEP_TIME = 915

    if path.exists(CACHE_CURSOR_FILE_NAME):
        with open(CACHE_CURSOR_FILE_NAME, 'r') as f:
            query = format_query(json.load(f)["endCursor"])
    else:
        query = format_query(None)

    query_result = client.execute(query)
    posts = flatten(query_result)
    while query_result["posts"]["pageInfo"]["hasNextPage"] == True and len(posts) < MAX_POSTS and MAX_REQUESTS > 0:
        if len(posts) % 200 == 0:
            print("{} posts retrieved. {} posts remaining. Sleeping for rate limit.".format(len(posts), MAX_POSTS - len(posts)))
            sleep(SLEEP_TIME)

        MAX_REQUESTS -= 1
        query = format_query(query_result["posts"]["pageInfo"]["endCursor"])
        query_result = client.execute(query)
        posts.extend(flatten(query_result))

    page_info = query_result["posts"]["pageInfo"]
    return (posts, page_info)


def sleep(sleep_time):
    for remaining in range(sleep_time, 0, -1):
        sys.stdout.write("\r")
        sys.stdout.write("{} seconds remaining.".format(remaining))
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write("\r                                     \r")


def format_query(end_cursor):
    if end_cursor != None:
        after = ", after: \"{}\"".format(end_cursor)
    else:
        after = ""

    query_str = '''{{
        posts(postedAfter: "2019-01-01T12:00:00Z", postedBefore: "2020-03-01T12:00:00Z", order: VOTES, first: 100{after}) {{
            edges {{
                node {{
                    id
                    createdAt
                    name
                    votesCount
                    commentsCount
                    reviewsRating
                    topics {{
                        edges {{
                            node {{
                                name
                            }}
                        }}
                    }}
                }}
            }}
            pageInfo {{
                startCursor
                endCursor
                hasNextPage
                hasPreviousPage
            }}
            totalCount
        }}
    }}
    '''.format(after=after)

    return gql(query_str)


def flatten(query_result):
    result = []

    for postEdge in query_result["posts"]["edges"]:
        post = postEdge["node"]
        topics = []

        for topicEdge in post["topics"]["edges"]:
            topics.append(topicEdge["node"]["name"])

        post["topics"] = topics
        result.append(post)

    return result
