from db import *
from toilet import *

def main():

    welcome()
    setup_env()
    conn = create_connection() 
    response = input("Do you want to create all the tables in DB? [y/n]: ")
    if(response == 'y'):
        create_all_tables(conn)
    response = input("\nDo you want to wrangle and insert all data into DB? [y/n]: ")
    if(response == 'y'):
        wrangle_and_insert()
    print("Finished! Now check for correctness...")


if __name__ == "__main__":
    main()
