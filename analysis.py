import sqlite3
import statistics
from collections import defaultdict
from pprint import pprint

import pandas as pd

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
    SELECT t.name, p.votes_count votes
    FROM post_topics m 
    JOIN topics t ON t.rowid = m.topic_id
    JOIN posts p on p.rowid = m.post_id
    '''
    c.execute(query)
    rows = c.fetchall()
    results = process(rows, 5, 50, 10)
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
        SELECT t.name, p.votes_count votes
        FROM post_topics m 
        JOIN topics t ON t.rowid = m.topic_id
        JOIN posts p on p.rowid = m.post_id
        WHERE p.createdAt > "{after}" AND p.createdAt < "{before}"
        '''.format(after=months[i], before=months[i + 1])
        c.execute(query)
        rows = c.fetchall()
        # results = process(rows, 5, 50, 3)
        # pprint(results)
        add_to_results(rows, months[i])

        print("==========")

    df = pd.DataFrame(monthly_results)
    df = df.transpose()
    df.to_csv('monthly_tags.tsv', encoding='utf-8', sep='\t')
    print("Done")


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


def process(rows, filter_count, bin_size, limit):
    grouped = defaultdict(list)
    for key, val in rows:
        grouped[key].append(val)

    stats = []
    for key, val in grouped.items():
        if len(val) < filter_count:
            continue

        stat = {}
        stat['name'] = key
        stat['count'] = len(val)
        stat['mean'] = statistics.mean(val)
        stat['mean_bin'] = int(round(float(stat['mean'])/bin_size)*bin_size)
        if len(val) > 1:
            stat['stdev'] = statistics.stdev(val)
        else:
            stat['stdev'] = None
        stats.append(stat)

    stats = sorted(stats, key=lambda i: i['stdev'])
    return stats[:limit]


monthly_results = {}


def add_to_results(rows, month):
    monthly_results[month] = {}
    for name, vote_count in rows:
        monthly_results[month][name] = vote_count


if __name__ == '__main__':
    main()
