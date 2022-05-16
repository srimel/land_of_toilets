import pandas as pd
from db import insert_df_rows_to_table
import re


def wrangle(df):
    df['first_name'] = df['full_name'].apply(get_first_name)
    df['first_name'] = df['first_name'].apply(get_preferred_first_name)
    df['last_name'] = df['full_name'].apply(get_last_name)
    df['odin_login'] = df['email'].apply(get_odin_login)
    return df[['first_name', 'last_name', 'odin_login', 'sub_section']]


def get_first_name(full_name):
    if len(full_name.split(", ")) > 2:
        raise RuntimeError("too many commas")
    return full_name.split(", ")[1]


def get_last_name(full_name):
    if len(full_name.split(", ")) > 2:
        raise RuntimeError("too many commas")
    return full_name.split(", ")[0]


def get_preferred_first_name(first_name):
    # the (.*) tells the re library to return just this part of the match
    preferred_first_name_regex = re.compile('\(Pref: (.*)\)')
    if re.search(preferred_first_name_regex, first_name):
        return re.findall(preferred_first_name_regex, first_name)[0]
    else:
        return first_name


def get_odin_login(email):
    email_regex = re.compile('^(.*)@pdx.edu$')
    return re.findall(email_regex, email)[0]


if __name__ == "__main__":
    df = pd.read_csv("class_roster.csv", header=0)

    #
    # Lab Step 4a
    # df.iloc[0] finds a DF location by index, column number default
    # df.loc[...] find's
    #
    # print(df.iloc[0])
    # print(df.loc[(df['favorite_color'] == 'blue') & (df['sub_section'] == 486)])

    #
    # Lab Step 4b
    #
    print(df.dtypes)

    #
    # Lab Step 6
    #
    # df = wrangle(df)
    # insert_df_rows_to_table(df, 'class_roster')
    # print(df)

