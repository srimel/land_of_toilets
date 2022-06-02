import psycopg2
from sqlalchemy import create_engine
from db import *
import os

def example_query(db_conn):
    cur = db_conn.cursor()
    cur.execute("SELECT * FROM toilets LIMIT 20;")
    print(cur.fetchall())   # <- what does this do? 

def run_all():
    setup_env()
    conn = create_connection() 
    example_query(conn)

if __name__ == "__main__":
    run_all()
