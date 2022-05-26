import psycopg2
from sqlalchemy import create_engine
import os


def welcome():
    print("\nLand Of Toilets - ETL\n")


#Used to setup the environment variables for the session
def setup_env():
    print("\nDo you need to setup environment variables?")
    if(input("[y/n] ") == 'y'):
        print("\nSetting up environment vars...")
        print("\nEnter Database Creditentials:")
        os.environ['CLASS_DB_HOST'] = 'dbclass.cs.pdx.edu'
        os.environ['CLASS_DB_USERNAME'] = input("User: ")
        os.environ['CLASS_DB_PASSWORD'] = input("PW: ")
        print("Done.\n")


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


# Create toilets table
def create_toilets_table(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE toilets(" \
                " FacilityID INT," \
                " URL varchar(256)," \
                " Name varchar(128)," \
                " Male bool,"\
                " Female bool,"\
                " Unisex bool,"\
                " AllGender bool,"\
                " ToiletNote varchar(1024),"\
                " DrinkingWater bool,"\
                " Shower bool,"\
                " PRIMARY KEY (FacilityID));"
    cur.execute(create_stmt)

# Create handicap table

# Create changing table

# Create access table

# Create disposal table

# Create dump_points table

# Create facility_types table


#######################################


# Create facility_rel table
def create_facility_rel(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE facility_rel (" \
                  "  FacilityID  INT," \
                  "  TypeID   INT," \
                  "  PRIMARY KEY (FacilityID, TypeID)," \
                  "  CONSTRAINT fk_type_id FOREIGN KEY(TypeID)," \
                  "  REFERENCES facility_types(TypeID);"
    cur.execute(create_stmt)

# Create locations table
def create_locations(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE locations (" \
                  "  LocID  INT," \
                  "  Adress1   VARCHAR(256)," \
                  "  Latitude   FLOAT," \
                  "  Longitude   FLOAT," \
                  "  PRIMARY KEY (LocID);"
    cur.execute(create_stmt)

# Create location_rel table
def create_location_rel(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE location_rel (" \
                  "  FacilityID  INT," \
                  "  LocID  INT," \
                  "  PRIMARY KEY (FacilityID, LocID," \
                  "  CONSTRAINT fk_loc_id FOREIGN KEY(LocID)," \
                  "  REFERENCES locations(LocID);"
    cur.execute(create_stmt)

# Create states table
def create_states(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE states (" \
                  "  StateID  INT," \
                  "  State  VARCHAR(16)," \
                  "  PRIMARY KEY (StateID);"
    cur.execute(create_stmt)

# Create state_rel table
def create_state_rel(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE state_rel (" \
                  "  LocID  INT," \
                  "  StateID  INT," \
                  "  PRIMARY KEY (LocID, StateID)," \
                  "  CONSTRAINT fk_loc_id FOREIGN KEY(LocID)," \
                  "  REFERENCES locations(LocID);"
    cur.execute(create_stmt)

# Create towns table
def create_towns(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE towns (" \
                  "  TownID  INT," \
                  "  Town  VARCHAR(128)," \
                  "  PRIMARY KEY (TownID);"
    cur.execute(create_stmt)

# Create town_rel table
def create_town_rel(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE town_rel (" \
                  "  LocID  INT," \
                  "  TownID  INT," \
                  "  PRIMARY KEY (LocID, TownID)," \
                  "  CONSTRAINT fk_loc_id FOREIGN KEY(LocID)," \
                  "  REFERENCES locations(LocID);"
    cur.execute(create_stmt)


if __name__ == "__main__":

    welcome()
    setup_env()

    conn = create_connection() 

    print("Test query:")
    example_query(conn)


##########################################################################


    # Lab Step 3
    #
    # example_query(conn)
    #
    # Lab Step 5
    #
    # create_class_roster_table(conn)
    # query_class_roster(conn)
