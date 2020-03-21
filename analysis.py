import sqlite3
from datetime import datetime

import pandas as pd

from posts_db import sqlite_file, topics_table_name


def main():
    analyze()


def analyze():
    conn = sqlite3.connect(sqlite_file)

    # all(conn)
    hourly(conn)

    # df = extract(conn)
    # aggregate(df)

    conn.close()


def extract(conn):
    query = '''
    SELECT p.rowid as p_rowid, p.name as p_name, p.votes_count, p.created_at, 
        t.rowid as t_rowid, t.name as t_name
    FROM post_topics m 
    JOIN topics t ON t.rowid = m.topic_id
    JOIN posts p on p.rowid = m.post_id;
    '''
    return pd.read_sql_query(query, conn)


def aggregate(df):
    topics_agg = df.groupby(['t_rowid'], as_index=False).agg(
        {'t_name': 'first', 'votes_count': ['count', 'mean', 'std']})
    topics_agg.columns = ['topic_id', 'topic_name',
                          'count', 'votes_mean', 'votes_std']

    # filter
    topics_agg = topics_agg[topics_agg['count'] > 9]

    # bin
    bins = range(0, 1200, 50)
    topics_agg['votes_mean_bin'] = pd.cut(topics_agg['votes_mean'], bins)

    # sort
    topics_agg.sort_values('count', ascending=False, inplace=True)
    print("Sorted by count")
    print(topics_agg.to_csv(sep="-", encoding="utf-8"))

    topics_agg.sort_values(['votes_mean', 'votes_std'],
                           ascending=[False, True], inplace=True)
    print("Sorted by votes mean, std")
    print(topics_agg.to_csv(sep="-", encoding="utf-8"))

    topics_agg.sort_values(['votes_mean_bin', 'votes_std'], ascending=[
                           False, True], inplace=True)
    print("Sorted by binned votes, std")
    print(topics_agg.to_csv(sep="-", encoding="utf-8"))

    topics_agg.sort_values('votes_std', inplace=True)
    print("Sorted by std")
    print(topics_agg.to_csv(sep="-", encoding="utf-8"))


def all(conn):
    query = '''
    SELECT rowid as rowid, name as name, votes_count, p.created_at
    FROM posts p;
    '''
    df = pd.read_sql_query(query, conn)
    df.to_csv('posts.tsv', sep='\t', encoding='utf-8')


def hourly(conn):
    query = '''
    SELECT rowid as rowid, name as name, votes_count, p.created_at
    FROM posts p;
    '''
    df = pd.read_sql_query(query, conn)

    df['hour_bin'] = pd.cut(pd.to_datetime(
        df['created_at']).dt.hour, bins=range(0, 25, 1), right=False)
    hourly = df.groupby(['hour_bin'], as_index=False).agg(
        {'votes_count': ['count', 'mean', 'std']})
    hourly.columns = ['hour_bin', 'count', 'votes_mean', 'votes_std']
    hourly['pct'] = 100 * hourly['count'] / hourly['count'].sum()
    hourly.to_csv('hourly.tsv', sep='\t', encoding='utf-8')


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
