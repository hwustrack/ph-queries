import json
import keyring
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport

CACHE_FILE_NAME = "posts.tsv"


def main():
    use_cache = True
    update_cache = True

    if (use_cache):
        with open(CACHE_FILE_NAME, 'r') as f:
                posts = json.load(f)
    else:
        posts = get_posts(update_cache)

    topics = {}
    for post in posts:
        for topic in post["topics"]:
            if topic in topics:
                topics[topic] += post["votesCount"]
            else:
                topics[topic] = post["votesCount"]

    maxKey = max(topics, key=topics.get)
    print(f"Max: {maxKey} - {topics[maxKey]}")


def get_posts(update_cache):
    auth_token = keyring.get_password('producthunt', 'auth')
    client = Client(
        transport=RequestsHTTPTransport(
            url='https://api.producthunt.com/v2/api/graphql',
            headers={'Authorization': 'Bearer ' + auth_token}
        )
    )

    query = gql('''
    {
        posts(postedAfter: "2020-02-21T12:00:00Z", order: RANKING, first: 10) {
            edges {
                node {
                    id
                    createdAt
                    name
                    votesCount
                    topics {
                        edges {
                            node {
                                name
                            }
                        }
                    }
                }
            }
        }
    }
    ''')

    query_result = client.execute(query)
    posts = flatten(query_result)

    if update_cache:
        with open(CACHE_FILE_NAME, 'w') as f:
            json.dump(posts, f, indent=4)

    return posts


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


if __name__ == '__main__':
    main()
