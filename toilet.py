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


data_file = 'test_data.csv' #'toilet_data.csv'


# Wranglin' Section

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


# Makes a unique int for each facility type (not UUID)
#
def wrangle_facility_types(df):
    facilities = []
    ids = []
    i = 1
    for item in df['FacilityType']:
        if item not in facilities:
            facilities.append(item)
            ids.append(i)
            i += 1
    new_table = {'TypeID' : ids, 'Name' : facilities}
    new_df = pd.DataFrame(new_table, columns = ['TypeID', 'Name'])
    return new_df


# This function is dependent on wrangle_facility_types() creating a 
# facility_types dataframe to be passed in as second argument. 
#
def wrangle_facility_rel(df, facility_type_df):
    type_ids = []
    for row in df['FacilityType']:
        j = 0 
        for row2 in facility_type_df['Name']:
            if row == row2:
                type_ids.append(facility_type_df['TypeID'].iloc[j])
                print("Row Match: " + row + " + " + row2)
                break;
            j += 1
    df['TypeID'] = type_ids
    return df[['FacilityID', 'TypeID']]


def wrangle_locations(df):

    loc_ids = []
    loc_tuples = []
    lat = []
    lon = []
    addr = []
    addr_notes = []

    i = 0
    for row in df['Address1']:
        # Tuple to determine uniqueness of a location
        loc = (df['Latitude'].iloc[i], df['Longitude'].iloc[i])

        if loc not in loc_tuples:
            loc_tuples.append(loc)
            loc_ids.append(i)
            addr.append(df['Address1'].iloc[i])
            lat.append(df['Latitude'].iloc[i])
            lon.append(df['Longitude'].iloc[i])
            addr_notes.append(df['AddressNote'].iloc[i])
            i += 1

    new_table = {'LocID': loc_ids, 'Address1' : addr, 'Latitude' : lat,
                 'Longitude' : lon, 'AddressNotes' : addr_notes}

    new_df = pd.DataFrame(new_table, columns = ['LocID', 'Address1',
                                                'Latitude', 'Longitude',
                                                'AddressNotes'])
    return new_df

        
if __name__ == "__main__":


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
    facility_types = wrangle_facility_types(df)
    facility_rel = wrangle_facility_rel(df, facility_types)
    locations = wrangle_locations(df)


    print("\nThe following dataframes were created:")
    print("Toilet Relation")
    print(toilets)
    print("\nHandicap Relation")
    print(handicap)
    print("\nChanging Relation")
    print(changing)
    print("\nAccess Relation")
    print(access)
    print("\nDisposal Relation")
    print(disposal)
    print("\nDump Point Relation")
    print(dump_points)
    print("\nFacility Types Relation")
    print(facility_types)
    print("\nFacility Relation")
    print(facility_rel)
    print("\nLocations Relation")
    print(locations)
    
    
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
