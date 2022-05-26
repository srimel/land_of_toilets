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
                " URL VARCHAR(256)," \
                " Name VARCHAR(128)," \
                " Male BOOL,"\
                " Female BOOL,"\
                " Unisex BOOL,"\
                " AllGender BOOL,"\
                " ToiletNote VARCHAR(1024),"\
                " DrinkingWater BOOL,"\
                " Shower BOOL,"\
                " PRIMARY KEY (FacilityID));"
    cur.execute(create_stmt)

# Create handicap table
def create_handicap_table(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE handicap(" \
                " FacilityID INT," \
                " BYOSling BOOL,"\
                " Ambulant BOOL,"\
                " LHTransfer BOOL,"\
                " RHTransfer BOOL,"\
                " PRIMARY KEY (FacilityID),"\
                " CONSTRAINT handicap_fk FOREIGN KEY(FacilityID) "\
                " REFERENCES toilets(facilityID));"
    cur.execute(create_stmt)

# Create changing table
def create_changing_table(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE changing(" \
                " FacilityID INT," \
                " BabyChange BOOL,"\
                " BabyCareRoom BOOL,"\
                " BabyChangeNote VARCHAR(256)"\
                " ACShower BOOL,"\
                " AdultChange BOOL,"\
                " AdultChangeNote VARCHAR(256),"\
                " ChangingPlaces BOOL,"\
                " PRIMARY KEY (FacilityID),"\
                " CONSTRAINT changing_fk FOREIGN KEY(FacilityID) "\
                " REFERENCES toilets(facilityID));"
    cur.execute(create_stmt)

# Create access table
def create_access_table(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE access(" \
                " FacilityID INT," \
                " KeyRequired BOOL,"\
                " AccessNote VARCHAR(128),"\
                " PaymentRequired BOOL,"\
                " MLAK24 BOOL,"\
                " MLAKAfterHours BOOL,"\
                " OpeningHours VARCHAR(256),"\
                " OpeningHoursNote VARCHAR(256),"\
                " Accessible BOOL,"\
                " Parking BOOL,"\
                " ParkingAccessible BOOL,"\
                " ParkingNote VARCHAR(256),"\
                " PRIMARY KEY (FacilityID),"\
                " CONSTRAINT access_fk FOREIGN KEY(FacilityID) "\
                " REFERENCES toilets(facilityID));"
    cur.execute(create_stmt)

# Create disposal table
def create_disposal_table(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE disposal(" \
                " FacilityID INT," \
                " SharpsDisposal BOOL,"\
                " SanitaryDisposal BOOL,"\
                " MensPadDisposal BOOL,"\
                " PRIMARY KEY (FacilityID),"\
                " CONSTRAINT disposal_fk FOREIGN KEY(FacilityID) "\
                " REFERENCES toilets(facilityID));"
    cur.execute(create_stmt)

# Create dump_points table
def create_dump_points_table(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE dump_points(" \
                " FacilityID INT," \
                " DPWashout BOOL,"\
                " DPAfterHours BOOL,"\
                " DumpPointNote VARCHAR(256),"\
                " PRIMARY KEY (FacilityID),"\
                " CONSTRAINT dump_points_fk FOREIGN KEY(FacilityID) "\
                " REFERENCES toilets(facilityID));"
    cur.execute(create_stmt)

# Create facility_types table
def create_facility_types_table(db_conn):
    cur = db_conn.cursor()
    create_stmt = "CREATE TABLE facility_types(" \
                " TypeID INT," \
                " Name VARCHAR(128));"
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
