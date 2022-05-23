import psycopg2
from sqlalchemy import create_engine
import os

#Used to setup the environment variables for the session
def setup_env(usr, pw):
    print("Setting up environment vars...")
    os.environ['CLASS_DB_HOST'] = 'dbclass.cs.pdx.edu'
    os.environ['CLASS_DB_USERNAME'] = usr
    os.environ['CLASS_DB_PASSWORD'] = pw
    print("Done.")

def create_connection():
    connection = psycopg2.connect(
        host=os.environ['CLASS_DB_HOST'],
        database=os.environ['CLASS_DB_USERNAME'],
        user=os.environ['CLASS_DB_USERNAME'],
        password=os.environ['CLASS_DB_PASSWORD']
    )
    connection.autocommit = True
    return connection


def example_query(db_conn):
    cur = db_conn.cursor()
    cur.execute("SELECT first, last FROM agent LIMIT 20;")
    print(cur.fetchall())

def my_query(db_conn):
    cur = db_conn.cursor()
    cur.execute("SELECT * FROM agent LIMIT 10;")
    print(cur.fetchall())

def create_class_roster_table(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE class_roster (" \
                  "  first_name  varchar(32)," \
                  "  last_name   varchar(32)," \
                  "  odin_login  varchar(32)," \
                  "  sub_section varchar(8));"
    cur.execute(create_stmt)


def insert_df_rows_to_table(df, table_name):
    engine = create_engine(
        f"postgresql://{os.environ['CLASS_DB_USERNAME']}:"
        f"{os.environ['CLASS_DB_PASSWORD']}@"
        f"{os.environ['CLASS_DB_HOST']}:"
        f"5432/{os.environ['CLASS_DB_USERNAME']}"
    )
    df.to_sql(table_name, engine, if_exists="append", index=False)


def query_class_roster(db_conn):
    cur = db_conn.cursor()
    cur.execute("SELECT * FROM class_roster;")
    print(cur.fetchall())


if __name__ == "__main__":

    # Uncomment this line to setup your environment variables
    setup_env(input("Enter User: "),input("Enter PW: "))

    conn = create_connection() 
    my_query(conn)

    # Lab Step 3
    #
    # example_query(conn)
    #
    # Lab Step 5
    #
    # create_class_roster_table(conn)
    # query_class_roster(conn)
