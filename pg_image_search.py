#!/usr/bin/env python3
import psycopg2

DB_NAME = 'image_data'
USER = 'postgres'
HOST = 'localhost'
PASSWORD = ''


def run_queries(conn):
    with conn.cursor() as cur:
        cur.execute('select 1')
        print(cur.fetchall())


def main():
    conn_opts = {
        'dbname': DB_NAME,
        'user': USER,
        'host': HOST,
        'password': PASSWORD
    }
    conn_str = ' '.join(["{k}='{v}'".format(k=k, v=v) for k, v in conn_opts.items()])
    with psycopg2.connect(conn_str) as conn:
        run_queries(conn)


if __name__ == '__main__':
    main()
