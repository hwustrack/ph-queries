import sqlite3
from pprint import pprint

from posts_db import sqlite_file, topics_table_name


def main():
    analyze()

def analyze():
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    query = '''
        SELECT t.name, count(m.topic_id) count, sum(p.votesCount) votes, 
            sum(p.votesCount) / count(m.topic_id) norm
        FROM post_topics m 
        JOIN topics t ON t.rowid = m.topic_id
        JOIN posts p on p.rowid = m.post_id
        GROUP BY m.topic_id
        ORDER BY norm DESC
        '''
    c.execute(query)
    results = c.fetchall()
    pprint(results)

    conn.close()

if __name__ == '__main__':
    main()
