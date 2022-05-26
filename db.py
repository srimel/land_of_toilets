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
                " FacilityID int," \
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
def create_handicap_table(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE handicap(" \
                " FacilityID int," \
                " BYOSling bool,"\
                " Ambulant bool,"\
                " LHTransfer bool,"\
                " RHTransfer bool,"\
                " PRIMARY KEY (FacilityID),"\
                " CONSTRAINT handicap_fk FOREIGN KEY(FacilityID) "\
                " REFERENCES toilets(facilityID));"
    cur.execute(create_stmt)

# Create changing table
def create_changing_table(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE changing(" \
                " FacilityID int," \
                " BabyChange bool,"\
                " BabyCareRoom bool,"\
                " BabyChangeNote varchar(256)"\
                " ACShower bool,"\
                " AdultChange bool,"\
                " AdultChangeNote varchar(256),"\
                " ChangingPlaces book,"\
                " PRIMARY KEY (FacilityID),"\
                " CONSTRAINT changing_fk FOREIGN KEY(FacilityID) "\
                " REFERENCES toilets(facilityID));"
    cur.execute(create_stmt)

# Create access table

# Create disposal table

# Create dump_points table

# Create facility_types table

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
