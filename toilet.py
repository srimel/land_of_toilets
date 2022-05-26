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


data_file = 'test_data.csv' #'toilet_data.csv' # #


# Wranglin' Section

def wrangle_toilets(df):
    attributes = ['facilityid', 'url', 'name', 'male', 'female', 'unisex', 
                  'allgender', 'toiletnote', 'drinkingwater', 'shower']
    return df[attributes]


def wrangle_handicap(df):
    attributes = ['facilityid', 'byosling', 'ambulant', 'lhtransfer', 
                  'rhtransfer']
    return df[attributes]


def wrangle_changing(df):
    attributes = ['facilityid', 'babychange', 'babycareroom', 
                  'babychangenote', 'acshower', 'adultchange',
                  'adultchangenote', 'changingplaces']
    return df[attributes]


def wrangle_access(df):
    attributes = ['facilityid', 'keyrequired', 'accessnote', 
                  'paymentrequired', 'mlak24', 'mlakafterhours', 
                  'openinghoursnote', 'accessible', 'parking', 
                  'parkingaccessible', 'parkingnote']
    return df[attributes]


def wrangle_disposal(df):
    attributes = ['facilityid', 'sharpsdisposal', 'sanitarydisposal', 
                  'menspaddisposal']
    return df[attributes]


def wrangle_dump_points(df):
    attributes = ['facilityid', 'dpwashout', 'dpafterhours', 
                  'dumppointnote']
    return df[attributes]


# Makes a unique int for each facility type (not UUID)
#
def wrangle_facility_types(df):
    facilities = []
    ids = []
    i = 1
    for item in df['facilitytype']:
        if item not in facilities:
            facilities.append(item)
            ids.append(i)
            i += 1
    new_table = {'typeid' : ids, 'name' : facilities}
    new_df = pd.DataFrame(new_table, columns = ['typeid', 'name'])
    return new_df


# This function is dependent on wrangle_facility_types() creating a 
# facility_types dataframe to be passed in as second argument. 
#
def wrangle_facility_rel(df, facility_type_df):
    type_ids = []
    for row in df['facilitytype']:
        j = 0 
        for row2 in facility_type_df['name']:
            if row == row2:
                type_ids.append(facility_type_df['typeid'].iloc[j])
                break;
            j += 1
    df['typeid'] = type_ids
    return df[['facilityid', 'typeid']]


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
    for row in df['address1']:
        # Tuple to determine uniqueness of a location
        loc = (df['latitude'].iloc[index], df['longitude'].iloc[index])
        if loc not in loc_tuples:
            loc_tuples.append(loc)
            loc_ids.append(new_id)
            addr.append(df['address1'].iloc[index])
            lat.append(df['latitude'].iloc[index])
            lon.append(df['longitude'].iloc[index])
            addr_notes.append(df['addressnote'].iloc[index])
            new_id += 1
        index += 1
    new_table = {'locid': loc_ids, 'address1' : addr, 'latitude' : lat,
                 'longitude' : lon, 'addressnote' : addr_notes}
    new_df = pd.DataFrame(new_table, columns = ['locid', 'address1',
                                                'latitude', 'longitude',
                                                'addressnote'])
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
    for row in df['facilityid']:
        j = 0
        for row2 in loc_df['locid']: 
            if (df['latitude'].iloc[i] == loc_df['latitude'].iloc[j]) and (df['longitude'].iloc[i] == loc_df['longitude'].iloc[j]):
                loc_ids.append(loc_df['locid'].iloc[j])
                count += 1
                break;
            j += 1
        if i % 367 == 0:
            print(".", end="", flush=True)
        i += 1
    print("Done!")
    #print("")
    df['locid'] = loc_ids
    # Can use count to error check if we have time
    return df[['facilityid', 'locid']]


def wrangle_states(df):
    states = []
    state_ids = []
    new_id = 1
    for row in df['state']:
        if row not in states:
            states.append(row)
            state_ids.append(new_id)
            new_id += 1
    new_table = {'stateid' : state_ids, 'state' : states}
    new_df = pd.DataFrame(new_table, columns = ['stateid', 'state'])
    return new_df


def wrangle_state_rel(df, states):
    loc_ids = []
    state_ids = []
    tuples = []
    i = 0
    for row in df['locid']:
        tup = (df['locid'].iloc[i], df['state'].iloc[i])
        if tup not in tuples:
            tuples.append(tup)
            loc_ids.append(df['locid'].iloc[i])
            st_id = get_state_id(states, df['state'].iloc[i])
            state_ids.append(st_id)
        i += 1
    new_table = {'locid' : loc_ids, 'stateid' : state_ids}
    new_df = pd.DataFrame(new_table, columns = ['locid', 'stateid'])
    return new_df


def get_state_id(states, key):
    i = 0
    for row in states['state']:
        if row == key:
            return states['stateid'].iloc[i]
        i += 1
    return 0


def wrangle_towns(df):
    towns = []
    town_ids = []
    new_id = 1
    for row in df['town']:
        if row not in towns:
            towns.append(row)
            town_ids.append(new_id)
            new_id += 1
    new_table = {'townid' : town_ids, 'town' : towns}
    new_df = pd.DataFrame(new_table, columns = ['townid', 'town'])
    return new_df


def wrangle_town_rel(df, towns):
    loc_ids = []
    town_ids = []
    tuples = []
    i = 0
    for row in df['locid']:
        tup = (df['locid'].iloc[i], df['town'].iloc[i])
        if tup not in tuples:
            tuples.append(tup)
            loc_ids.append(df['locid'].iloc[i])
            t_id = get_town_id(towns, df['town'].iloc[i])
            town_ids.append(t_id)
        i += 1
    new_table = {'locid' : loc_ids, 'townid' : town_ids}
    new_df = pd.DataFrame(new_table, columns = ['locid', 'townid'])
    return new_df


def get_town_id(towns, key):
    i = 0
    for row in towns['town']:
        if row == key:
            return towns['townid'].iloc[i]
        i += 1
    return 0

# Requires to setup environment vars before running this 
# wrangles raw data_file into 14 dataframes based off our schemas, then
# inserts by row into each respective relation.
def wrangle_and_insert():

    print("\nReading CSV into panda's dataframe...")
    df = pd.read_csv(data_file, header=0)
    df.columns = map(str.lower, df.columns) #this was certainly handy...

    print("\nWrangling data...")
    toilets = wrangle_toilets(df)
    insert_df_rows_to_table(toilets, 'toilets')
    print("Got the toilets dataframe!")
    handicap = wrangle_handicap(df)
    insert_df_rows_to_table(handicap, 'handicap')
    print("Got the handicap dataframe!")
    changing = wrangle_changing(df)
    insert_df_rows_to_table(changing, 'changing')
    print("Got the changing dataframe!")
    access = wrangle_access(df)
    insert_df_rows_to_table(access, 'access')
    print("Got the access dataframe!")
    disposal = wrangle_disposal(df)
    insert_df_rows_to_table(disposal, 'disposal')
    print("Got the disposal dataframe!")
    dump_points = wrangle_dump_points(df)
    insert_df_rows_to_table(dump_points, 'dump_points')
    print("Got the dump_points dataframe!")
    facility_types = wrangle_facility_types(df)
    insert_df_rows_to_table(facility_types, 'facility_types')
    print("Got the facillity_types dataframe!")
    facility_rel = wrangle_facility_rel(df, facility_types)
    insert_df_rows_to_table(facility_rel, 'facility_rel')
    print("Got the facillity_rel dataframe!")
    locations = wrangle_locations(df)
    insert_df_rows_to_table(locations, 'locations')
    print("Got the locations dataframe!")
    location_rel = wrangle_location_rel(df, locations)
    insert_df_rows_to_table(location_rel, 'location_rel')
    print("Got the location_rel dataframe!")
    states = wrangle_states(df)
    insert_df_rows_to_table(states, 'states')
    print("Got the states dataframe!")
    state_rel = wrangle_state_rel(df,states)
    insert_df_rows_to_table(state_rel, 'state_rel')
    print("Got the state_rel dataframe!")
    towns = wrangle_towns(df)
    insert_df_rows_to_table(towns, 'towns')
    print("Got the towns dataframe!")
    town_rel = wrangle_town_rel(df,towns)
    insert_df_rows_to_table(town_rel, 'town_rel')
    print("Got the town_rel dataframe!")



if __name__ == "__main__":
    #wrangle_all()




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
