#!/usr/bin/env python3
import math
import random
import time

import psycopg2

DB_NAME = 'image_data'
USER = 'postgres'
HOST = 'localhost'
PASSWORD = ''


def ensure_tables(conn):
    queries = """
    CREATE EXTENSION IF NOT EXISTS cube;
    CREATE EXTENSION IF NOT EXISTS pgcrypto;
    
    CREATE TABLE IF NOT EXISTS image_data (
      uid text NOT NULL PRIMARY KEY,
      data_arr real[128],
      created_at timestamptz not null default now()
    );
    
    -- this will generate uid
    -- TODO we should decide on how to make use of ordering
    CREATE OR REPLACE FUNCTION sha256(real[]) returns text AS $$
      SELECT encode(digest(array_to_string($1, ','), 'sha256'), 'hex')
    $$ LANGUAGE SQL STRICT IMMUTABLE;
    
    CREATE OR REPLACE FUNCTION gen_id() RETURNS trigger AS $$
    BEGIN
        NEW.uid := sha256(NEW.data_arr);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    
    DROP TRIGGER IF EXISTS trigger_gen_id on image_data;
    CREATE TRIGGER trigger_gen_id BEFORE INSERT ON image_data
    FOR EACH ROW EXECUTE PROCEDURE gen_id();
    
    CREATE OR REPLACE FUNCTION knn(real[], INT) returns SETOF image_data AS $$
      SELECT 
        uid,
        data_arr, 
        created_at
      FROM image_data
      ORDER BY cube_distance(cube($1), cube(data_arr)) ASC
      LIMIT $2
    $$ LANGUAGE SQL STRICT IMMUTABLE ROWS 10;
    """

    with conn.cursor() as cur:
        cur.execute(queries)
        print('successfully created/ensured tables')


def gen_row(dims=128):
    return [random.uniform(0.0, 1.0) for _ in range(dims)]


def fill_data(conn):
    with conn.cursor() as cur:
        rows = [gen_row() for _ in range(1000)]
        for i, row in enumerate(rows):
            if i % 100 == 0:
                print('inserted', i, 'rows')
            cur.execute('insert into image_data (data_arr) values (%s)', (row,))


def distance(r1, r2):
    assert len(r1) == len(r2), 'mismatch: {} {}'.format(r1, r2)
    return math.sqrt(sum(map(lambda u: (u[0] - u[1]) ** 2, zip(r1, r2))))


def run_queries(conn, n=10):
    with conn.cursor() as cur:
        cur.execute('select count(*) from image_data')
        print('current count is', cur.fetchone())

        results = []
        timings = []
        gens = []
        for _ in range(n):
            gen = gen_row()
            gens.append(gen)

            a = time.time()
            cur.callproc('knn', (gen, 10))
            results.append([r for r in cur])
            t = time.time() - a

            timings.append(t)

        print('average timing is', sum(timings) / len(timings))
        print('results are (note they are increasing):')
        for i, top_n in enumerate(results):
            print([distance(gens[i], x[1]) for x in top_n])


def main():
    conn_opts = {
        'dbname': DB_NAME,
        'user': USER,
        'host': HOST,
        'password': PASSWORD
    }
    conn_str = ' '.join(["{k}='{v}'".format(k=k, v=v) for k, v in conn_opts.items()])
    with psycopg2.connect(conn_str) as conn:
        ensure_tables(conn)
        fill_data(conn)
        run_queries(conn)


if __name__ == '__main__':
    main()
