import sqlite3
from pprint import pprint

from posts_db import sqlite_file, topics_table_name


def main():
    analyze()


def analyze():
    conn = sqlite3.connect(sqlite_file)

    annual(conn)
    monthly(conn, get_months())

    conn.close()

def annual(conn):
    c = conn.cursor()

    print("===== Annual =====")

    # topics with most posts
    query = '''
    SELECT t.name, count(m.topic_id) count
    FROM post_topics m 
    JOIN topics t ON t.rowid = m.topic_id
    JOIN posts p on p.rowid = m.post_id
    GROUP BY m.topic_id
    ORDER BY count DESC
    LIMIT 10;
    '''
    c.execute(query)
    results = c.fetchall()
    pprint(results)

    # topics with most average votes
    query = '''
    SELECT t.name, count(m.topic_id) count, sum(p.votes_count) votes, 
        sum(p.votes_count) / count(m.topic_id) mean
    FROM post_topics m 
    JOIN topics t ON t.rowid = m.topic_id
    JOIN posts p on p.rowid = m.post_id
    GROUP BY m.topic_id
    HAVING count > 4
    ORDER BY mean DESC
    LIMIT 10;
    '''
    c.execute(query)
    results = c.fetchall()
    pprint(results)

    print("==========")

def monthly(conn, months):
    c = conn.cursor()

    print("===== Monthly =====")

    for i in range(len(months) - 1):
        print(months[i])

        # topics with most posts
        query = '''
        SELECT t.name, count(m.topic_id) count
        FROM post_topics m 
        JOIN topics t ON t.rowid = m.topic_id
        JOIN posts p on p.rowid = m.post_id
        WHERE p.createdAt > "{after}" AND p.createdAt < "{before}"
        GROUP BY m.topic_id
        ORDER BY count DESC
        LIMIT 3;
        '''.format(after=months[i], before=months[i + 1])
        c.execute(query)
        results = c.fetchall()
        pprint(results)

        # topics with most average votes
        query = '''
        SELECT t.name, count(m.topic_id) count, sum(p.votes_count) votes, 
            sum(p.votes_count) / count(m.topic_id) mean
        FROM post_topics m 
        JOIN topics t ON t.rowid = m.topic_id
        JOIN posts p on p.rowid = m.post_id
        WHERE p.createdAt > "{after}" AND p.createdAt < "{before}"
        GROUP BY m.topic_id
        HAVING COUNT > 4
        ORDER BY mean DESC
        LIMIT 3;
        '''.format(after=months[i], before=months[i + 1])
        c.execute(query)
        results = c.fetchall()
        pprint(results)

        query = '''
        SELECT count(*)
        FROM posts
        WHERE createdAt > "{after}" AND createdAt < "{before};"
        '''.format(after=months[i], before=months[i + 1])
        c.execute(query)
        results = c.fetchall()
        pprint(results)

        print("==========")


def get_months():
    months = []

    year = "2019"
    for m in range(12):
        m = m + 1
        if m < 10:
            months.append(year + "-0" + str(m) + "-01T00:00:00Z")
        else:
            months.append(year + "-" + str(m) + "-01T00:00:00Z")

    year = "2020"
    for m in range(3):
        m = m + 1
        months.append(year + "-0" + str(m) + "-01T00:00:00Z")

    return months


if __name__ == '__main__':
    main()
