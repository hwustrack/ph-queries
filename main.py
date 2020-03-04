import json
import keyring
import statistics
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from posts_db import insert_posts

CACHE_FILE_NAME = "posts.json"


def main():
    use_cache = True
    update_cache = True

    if (use_cache):
        with open(CACHE_FILE_NAME, 'r') as f:
                posts = json.load(f)
    else:
        posts = get_posts(update_cache)

    insert_posts(posts)

    # topics = {}
    # for post in posts:
    #     for topic in post["topics"]:
    #         if topic in topics:
    #             topics[topic].append(post["votesCount"])
    #         else:
    #             topics[topic] = [post["votesCount"]]

    # topic_means = {}
    # for topic in topics.keys():
    #     topic_means[topic] = statistics.mean(topics[topic])

    # sorted_topic_means = {k: v for k, v in sorted(topic_means.items(), key=lambda item: item[1], reverse=True)}
    # print(sorted_topic_means)


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
        posts(postedAfter: "2020-02-01T12:00:00Z", order: RANKING, first: 100) {
            edges {
                node {
                    id
                    createdAt
                    name
                    votesCount
                    commentsCount
                    reviewsRating
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
