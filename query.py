import psycopg2
from sqlalchemy import create_engine
import os

def example_query(db_conn):
    cur = db_conn.cursor()
    cur.execute("SELECT * FROM toilets;")
    print(cur.fetchall())   # <- what does this do?