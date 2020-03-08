import statistics

import keyring

from ph_client import get_posts
from posts_db import insert_posts


def main():
    use_cache = False
    update_cache = True

    posts = get_posts(keyring.get_password(
        'producthunt', 'auth'), use_cache, update_cache)
    insert_posts(posts)


if __name__ == '__main__':
    main()
