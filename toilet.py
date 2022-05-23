import pandas as pd
from db import insert_df_rows_to_table
import re

# Australia, Land of Toilets - ETL Application
# by Stuart Rimel, Pete Wells
#
# This will be the main file we run for our ETL. First, the program will
# read from the .csv and store into panda dataframe. From there we can
# wrangle the data into separate dataframes based off our schemas. This
# file will then need to import some yet-to-be-made function from db.py
# that creates all the tables in SQL that we will be inserting into. After
# table creation happens, we will invoke the "insert_df_rows_to_table"
# function from db.py which should insert our data into the table in bulk.
# Or so we hope...


data_file = 'test_data.csv'


def wrangle_toilets(df):
    attributes = ['FacilityID', 'Name', 'Male', 'Female', 'Unisex', 
                  'AllGender', 'ToiletNote', 'DrinkingWater', 'Shower']
    return df[attributes]


def wrangle_handicap(df):
    attributes = ['FacilityID', 'BYOSling', 'Ambulant', 'LHTransfer', 
                  'RHTransfer']
    return df[attributes]


def wrangle_changing(df):
    attributes = ['FacilityID', 'BabyChange', 'BabyCareRoom', 
                  'BabyChangeNote', 'ACShower', 'AdultChange',
                  'AdultChangeNote', 'ChangingPlaces']
    return df[attributes]


def wrangle_access(df):
    attributes = ['FacilityID', 'KeyRequired', 'AccessNote', 
                  'PaymentRequired', 'MLAK24', 'MLAKAfterHours', 
                  'OpeningHoursNote', 'Accessible', 'Parking', 
                  'ParkingAccessible', 'ParkingNote']
    return df[attributes]

def wrangle_disposal(df):
    attributes = ['FacilityID', 'SharpsDisposal', 'SanitaryDisposal', 
                  'MensPadDisposal']
    return df[attributes]

def wrangle_dump_points(df):
    attributes = ['FacilityID', 'DPWashout', 'DPAfterHours', 
                  'DumpPointNote']
    return df[attributes]

if __name__ == "__main__":

    # Reading from test toilet data (9 rows)
    print("\nReading CSV into panda's dataframe...")

    df = pd.read_csv(data_file, header=0)

    print("\nDisplaying original loaded dataframe:")
    print(df)

    print("\nWrangling data...")
    toilets = wrangle_toilets(df)
    handicap = wrangle_handicap(df)
    changing = wrangle_changing(df)
    access = wrangle_access(df)
    disposal = wrangle_disposal(df)
    dump_points = wrangle_dump_points(df)

    print("\nThe following dataframes were created:")
    print("Toilet Relation")
    print(toilets)
    print("Handicap Relation")
    print(handicap)
    print("Changing Relation")
    print(changing)
    print("Access Relation")
    print(access)
    print("Disposal Relation")
    print(disposal)
    print("Dump Point Relation")
    print(dump_points)
    
    


#########################################################################


    # Lab Step 4a
    # df.iloc[0] finds a DF location by index, column number default
    # df.loc[...] find's
    #
    # print(df.iloc[0])
    # print(df.loc[(df['favorite_color'] == 'blue') & (df['sub_section'] == 486)])

    # Lab Step 4b
    #
    # print(df.dtypes)

    # Lab Step 6
    #
    # df = wrangle(df)
    # insert_df_rows_to_table(df, 'class_roster')
    # print(df)

# 
#def wrangle(df):
    #df['first_name'] = df['full_name'].apply(get_first_name)
    #df['first_name'] = df['first_name'].apply(get_preferred_first_name)
    #df['last_name'] = df['full_name'].apply(get_last_name)
    #df['odin_login'] = df['email'].apply(get_odin_login)
#    return df[['FacilityID', 'Town']]

#def get_first_name(full_name):
#    if len(full_name.split(", ")) > 2:
#        raise RuntimeError("too many commas")
#    return full_name.split(", ")[1]

#def get_last_name(full_name):
#    if len(full_name.split(", ")) > 2:
#        raise RuntimeError("too many commas")
#    return full_name.split(", ")[0]

#def get_preferred_first_name(first_name):
#    # the (.*) tells the re library to return just this part of the match
#    preferred_first_name_regex = re.compile('\(Pref: (.*)\)')
#    if re.search(preferred_first_name_regex, first_name):
#        return re.findall(preferred_first_name_regex, first_name)[0]
#    else:
#        return first_name

#def get_odin_login(email):
#    email_regex = re.compile('^(.*)@pdx.edu$')
#    return re.findall(email_regex, email)[0]
