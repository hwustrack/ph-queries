import sqlite3
from posts_db import sqlite_file
from posts_db import topics_table_name

def main():
    analyze()

def analyze():
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()

    conn.close()

if __name__ == '__main__':
    main()
