import sqlite3

sqlite_file = 'posts.db'

posts_table_name = 'posts'
posts_columns = {
    'id': 'INTEGER',
    'createdAt': 'TEXT',
    'name': 'TEXT',
    'votes_count': 'INTEGER',
    'comments_count': 'INTEGER',
    'reviews_rating': 'REAL'
}

topics_table_name = 'topics'
topics_columns = {
    'name': 'TEXT'
}

post_topics_table_name = 'post_topics'
post_topics_columns = {
    'post_id': 'INTEGER',
    'topic_id': 'INTEGER'
}


def main():
    create_schema()


def create_schema():
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    query = create_table_query(posts_table_name, posts_columns)
    c.execute(query)
    c.execute('CREATE UNIQUE INDEX idx_posts_id ON posts(id)')

    query = create_table_query(topics_table_name, topics_columns)
    c.execute(query)
    c.execute('CREATE UNIQUE INDEX idx_topics_name ON topics(name)')

    query = create_table_query(post_topics_table_name, post_topics_columns)
    c.execute(query)
    c.execute(
        'CREATE UNIQUE INDEX idx_post_topics_ids ON post_topics(post_id,topic_id)')

    conn.commit()
    conn.close()


def create_table_query(table_name, columns_dict):
    query = 'CREATE TABLE {tn} ('.format(tn=table_name)
    for column_name, column_type in columns_dict.items():
        query += '{cn} {ct},'.format(cn=column_name, ct=column_type)
    query = query[:-1]
    query += ')'
    return query


def insert_posts(posts):
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    for post in posts:
        insert_posts_query = "INSERT INTO {pt} (id, createdAt, name, votes_count, comments_count, reviews_rating)"\
            " VALUES(?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING"\
            .format(pt=posts_table_name)
        c.execute(insert_posts_query,
                  (post['id'], post['createdAt'], post['name'], post['votesCount'], post['commentsCount'], post['reviewsRating']))
        conn.commit()
        post_id = c.lastrowid

        for topic in post['topics']:
            insert_topics_query = "INSERT INTO {tt} (name) VALUES(?) ON CONFLICT DO NOTHING".format(
                tt=topics_table_name)
            c.execute(insert_topics_query, (topic, ))
            conn.commit()
            c.execute("SELECT rowid FROM {tt} WHERE name = ?".format(
                tt=topics_table_name), (topic, ))
            topic_id = c.fetchone()[0]

            insert_post_topics_query = 'INSERT INTO {ptt} (post_id, topic_id) VALUES(?, ?)'\
                .format(ptt=post_topics_table_name)
            c.execute(insert_post_topics_query, (post_id, topic_id))
            conn.commit()

        conn.commit()

    conn.commit()
    conn.close()


def get_last_rowid(c):
    return c.execute('SELECT last_insert_rowid()')


if __name__ == '__main__':
    main()
