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
                break;
            j += 1
    df['TypeID'] = type_ids
    return df[['FacilityID', 'TypeID']]


#Assumption is every location id has a unique lat/longitude?
def wrangle_locations(df):
    loc_ids = []
    loc_tuples = []
    lat = []
    lon = []
    addr = []
    addr_notes = []
    index = 0
    new_id = 1
    for row in df['Address1']:
        # Tuple to determine uniqueness of a location
        loc = (df['Latitude'].iloc[index], df['Longitude'].iloc[index])
        if loc not in loc_tuples:
            loc_tuples.append(loc)
            loc_ids.append(new_id)
            addr.append(df['Address1'].iloc[index])
            lat.append(df['Latitude'].iloc[index])
            lon.append(df['Longitude'].iloc[index])
            addr_notes.append(df['AddressNote'].iloc[index])
            new_id += 1
        index += 1
    new_table = {'LocID': loc_ids, 'Address1' : addr, 'Latitude' : lat,
                 'Longitude' : lon, 'AddressNotes' : addr_notes}
    new_df = pd.DataFrame(new_table, columns = ['LocID', 'Address1',
                                                'Latitude', 'Longitude',
                                                'AddressNotes'])
    return new_df

# This function is a performance bottleneck. With our current input size being
# 22,031 rows, this algorithm will take approximately 45 minutes to complete. 
# DMV version. 
def wrangle_location_rel(df, loc_df):
    i = 0
    ld_buff_size = 367
    count = 0
    loc_ids = []
    print("\nOh crap, a bottleneck!")
    print("\nAttempting to wrangle location_rel.")
    print("(~45min runtime on linux servers)")
    print("Wrangling.", end="", flush=True)
    for row in df['FacilityID']:
        j = 0
        for row2 in loc_df['LocID']: 
            if (df['Latitude'].iloc[i] == loc_df['Latitude'].iloc[j]) and (df['Longitude'].iloc[i] == loc_df['Longitude'].iloc[j]):
                loc_ids.append(loc_df['LocID'].iloc[j])
                count += 1
                break;
            j += 1
        if i % 367 == 0:
            print(".", end="", flush=True)
        i += 1
    print("")
    df['LocID'] = loc_ids
    # Can use count to error check if we have time
    return df[['FacilityID', 'LocID']]


def wrangle_states(df):
    states = []
    state_ids = []
    new_id = 1
    for row in df['State']:
        if row not in states:
            states.append(row)
            state_ids.append(new_id)
            new_id += 1
    new_table = {'StateID' : state_ids, 'State' : states}
    new_df = pd.DataFrame(new_table, columns = ['StateID', 'State'])
    return new_df


def wrangle_state_rel(df, states):
    loc_ids = []
    state_ids = []
    tuples = []
    i = 0
    for row in df['LocID']:
        tup = (df['LocID'].iloc[i], df['State'].iloc[i])
        if tup not in tuples:
            tuples.append(tup)
            loc_ids.append(df['LocID'].iloc[i])
            st_id = get_state_id(states, df['State'].iloc[i])
            state_ids.append(st_id)
        i += 1
    new_table = {'LocID' : loc_ids, 'StateID' : state_ids}
    new_df = pd.DataFrame(new_table, columns = ['LocID', 'StateID'])
    return new_df


def get_state_id(states, key):
    i = 0
    for row in states['State']:
        if row == key:
            return states['StateID'].iloc[i]
        i += 1
    return 0


def wrangle_towns(df):
    towns = []
    town_ids = []
    new_id = 1
    for row in df['Town']:
        if row not in towns:
            towns.append(row)
            town_ids.append(new_id)
            new_id += 1
    new_table = {'TownID' : town_ids, 'Town' : towns}
    new_df = pd.DataFrame(new_table, columns = ['TownID', 'Town'])
    return new_df


def wrangle_town_rel(df, towns):
    loc_ids = []
    town_ids = []
    tuples = []
    i = 0
    for row in df['LocID']:
        tup = (df['LocID'].iloc[i], df['Town'].iloc[i])
        if tup not in tuples:
            tuples.append(tup)
            loc_ids.append(df['LocID'].iloc[i])
            t_id = get_town_id(towns, df['Town'].iloc[i])
            town_ids.append(t_id)
        i += 1
    new_table = {'LocID' : loc_ids, 'TownID' : town_ids}
    new_df = pd.DataFrame(new_table, columns = ['LocID', 'TownID'])
    return new_df


def get_town_id(towns, key):
    i = 0
    for row in towns['Town']:
        if row == key:
            return towns['TownID'].iloc[i]
        i += 1
    return 0

def wrangle_all():
    print("\nReading CSV into panda's dataframe...")
    df = pd.read_csv(data_file, header=0)

    print("\nWrangling data...")
    toilets = wrangle_toilets(df)
    print("Got the toilets dataframe!")
    handicap = wrangle_handicap(df)
    print("Got the handicap dataframe!")
    changing = wrangle_changing(df)
    print("Got the changing dataframe!")
    access = wrangle_access(df)
    print("Got the access dataframe!")
    disposal = wrangle_disposal(df)
    print("Got the disposal dataframe!")
    dump_points = wrangle_dump_points(df)
    print("Got the dump_points dataframe!")

    facility_types = wrangle_facility_types(df)
    print("Got the facillity_types dataframe!")
    facility_rel = wrangle_facility_rel(df, facility_types)
    print("Got the facillity_rel dataframe!")

    locations = wrangle_locations(df)
    print("Got the locations dataframe!")
    location_rel = wrangle_location_rel(df, locations)
    print("Got the location_rel dataframe!")

    states = wrangle_states(df)
    print("Got the states dataframe!")
    state_rel = wrangle_state_rel(df,states)
    print("Got the state_rel dataframe!")

    towns = wrangle_towns(df)
    print("Got the towns dataframe!")
    town_rel = wrangle_town_rel(df,towns)
    print("Got the town_rel dataframe!")



if __name__ == "__main__":
    wrangle_all()




    '''
    print("\nThe following dataframes were created:")
    print("Toilets")
    print(toilets)
    print("\nHandicap")
    print(handicap)
    print("\nChanging")
    print(changing)
    print("\nAccess")
    print(access)
    print("\nDisposal")
    print(disposal)
    print("\nDump Point")
    print(dump_points)
    print("\nFacility Types")
    print(facility_types)
    print("\nFacility-Rel")
    print(facility_rel)
    print("\nLocations")
    print(locations)
    print("\nLocation-Rel")
    print(location_rel)
    print("\nStates")
    print(states)
    print("\nState-Rel")
    print(state_rel)
    print("\nTowns")
    print(towns)
    print("\nTown-Rel")
    print(town_rel)
    '''

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
