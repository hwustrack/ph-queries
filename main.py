import keyring
import pandas as pd
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport


def main():
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
                    description
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
    df = pd.DataFrame(posts)
    df.to_csv("posts.tsv", encoding='utf-8', sep="\t")

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
