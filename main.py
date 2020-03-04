import statistics

import keyring

from ph_client import get_posts
from posts_db import insert_posts

CACHE_FILE_NAME = "posts.json"


def main():
    use_cache = False
    update_cache = True
    posts = get_posts(keyring.get_password('producthunt', 'auth'), use_cache, update_cache, CACHE_FILE_NAME)

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


if __name__ == '__main__':
    main()
