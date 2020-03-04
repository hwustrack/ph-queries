import json
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport


def get_posts(auth_token, use_cache, update_cache, cache_file_name):
    if (use_cache):
        with open(cache_file_name, 'r') as f:
            posts = json.load(f)
    else:
        posts = get_posts_ph(auth_token)

    if not use_cache and update_cache:
        with open(cache_file_name, 'w') as f:
            json.dump(posts, f, indent=4)

    return posts


def get_posts_ph(auth_token):
    client = Client(
        transport=RequestsHTTPTransport(
            url='https://api.producthunt.com/v2/api/graphql',
            headers={'Authorization': 'Bearer ' + auth_token}
        )
    )

    query = format_query(None)
    query_result = client.execute(query)
    posts = flatten(query_result)
    MAX_REQUESTS = 450
    MAX_POSTS = 100
    while query_result["posts"]["pageInfo"]["hasNextPage"] == True and len(posts) < MAX_POSTS and MAX_REQUESTS > 0:
        MAX_REQUESTS -= 1
        query = format_query(query_result["posts"]["pageInfo"]["endCursor"])
        query_result = client.execute(query)
        posts.extend(flatten(query_result))

    return posts


def format_query(end_cursor):
    if end_cursor != None:
        after = ", after: \"{}\"".format(end_cursor)
    else:
        after = ""

    query_str = '''{{
        posts(postedAfter: "2020-02-01T12:00:00Z", order: VOTES, first: 100{after}) {{
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
