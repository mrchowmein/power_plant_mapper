import pandas as pd
import numpy as np
import re
import os


def filter_by_intersection(df_to_filter, col_to_filter, df_col_to_filter, def2_col):
    intersection = list(np.intersect1d(df_col_to_filter, def2_col))
    df_to_filter = df_to_filter[df_to_filter[f'{col_to_filter}'].isin(intersection)]

    return df_to_filter


def create_csv(output_df, file_name, all_columns=False):
    # Function returns a csv.
    print(f"Saving output as {file_name}.csv")
    if not all_columns:
        output_df = output_df[['entso_unit_id', 'platts_unit_id', 'gppd_plant_id']]
    output_df.to_csv(f"{file_name}.csv", index=False)


def clean_entso(input_df):
    # Function returns a cleaned entso df.
    # Function reformat strings so string cases match.
    # also strips strings of symbols and normalizes foreign language

    input_df['country'] = input_df['country'].str.split('(').str[0].str.upper().str.strip()
    # ideally, for the future use a lookup dict for country names if list is available
    platts_country_dict = {'UNITED KINGDOM': 'ENGLAND & WALES'}
    input_df['country_platts'] = input_df['country'].replace(platts_country_dict)
    input_df['unit_name'] = input_df['unit_name'].str.upper()
    input_df['unit_fuel'] = entso_df['unit_fuel'].str.lower()
    input_df['plant_name'] = input_df['plant_name'].str.normalize('NFKD').str.encode('ascii',
                                                                                     errors='ignore').str.decode(
        'utf-8')
    input_df['plant_name'] = input_df['plant_name'].str.upper()
    input_df['unit_name'] = input_df['unit_name'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]+', ' ', x))

    return input_df

def clean_platts(input_df):
    # Function returns a cleaned platts df.
    # Function reformat strings so string cases match.
    # also strips strings of symbols and normalizes foreign language

    input_df['UNIT'] = input_df['UNIT'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]+', ' ', x))
    input_df['PLANT'] = input_df['PLANT'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]+', ' ', x))
    input_df['plant_id'] = input_df['plant_id'].apply(str)


    return input_df


def clean_gppd(input_df):
    # Function returns a cleaned fppd df.
    # Function reformat strings so string cases match.
    # also strips strings of symbols and normalizes foreign language

    input_df['plant_name'] = input_df['plant_name'].str.upper()
    input_df['country_long'] = input_df['country_long'].str.upper()
    input_df['plant_primary_fuel'] = input_df['plant_primary_fuel'].str.upper()
    input_df['plant_name'] = input_df['plant_name'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]+', ' ', x))

    return input_df


def add_fuel_thesaurus(input_df, fuel_thesaurus_df):
    # function returns df with feul_thesaurus
    input_df = input_df.merge(fuel_thesaurus_df, left_on='unit_fuel', right_on='unit_fuel_platts_entsoe')
    input_df['plant_primary_fuel_gppd'] = input_df['plant_primary_fuel_gppd'].str.upper()

    return input_df

def process_entso(input_df):
    # Function finds the dominate name in the plant_name col. Drops unused cols and rename output columns
    input_df['plant_name'] = input_df.apply(lambda row: max(row.plant_name.split(), key=len), axis=1)
    #input_df = input_df.drop(['plant_capacity_mw', 'unit_fuel_platts_entsoe', 'note'], axis=1)
    input_df.rename(columns={'unit_id': 'entso_unit_id'}, inplace=True)

    return input_df


def process_platts(input_df):
    # Function finds the dominate name in the PLANT col. Drops unused cols and rename output columns
    input_df['PLANT'] = input_df.apply(lambda row: max(row.PLANT.split(), key=len), axis=1)
    input_df.rename(columns={'unit_id': 'platts_unit_id'}, inplace=True)
    #input_df = input_df.drop(['SUBREGION', 'AREA', 'STATE', 'CITY', 'YEAR'], axis=1)

    return input_df

def process_gppd(input_df):
    # Function finds the dominate name in the plant_name col.
    gppd_df.rename(columns={'plant_id': 'gppd_plant_id'}, inplace=True)

    return gppd_df

def merge_power_plant_dfs(entso_df, platts_df, gppd_df):
    #plant_name, fuel, and country
    output_df = entso_df.merge(platts_df, left_on=['plant_name', 'plant_primary_fuel_gppd', 'country'],
                               right_on=['PLANT', 'UNIT_FUEL', 'COUNTRY'], how='inner')

    # tie breakers to narrow down the multiple similar matches by checking the
    # 1. dominate word in the unit_name.
    # 2. checking for matching suffixes.
    # 3. checking if the unit_capacity_mw are within +/- 20%
    output_df['unit_match'] = output_df.apply(lambda row: max(row.unit_name.split(), key=len) in row.UNIT and
                                            row.unit_name[-1] == row.UNIT[-1] and
                                            (row.unit_capacity_mw * .8 < row.UNIT_CAPACITY_MW < row.unit_capacity_mw * 1.2),
                                            axis=1)

    output_df = output_df[output_df['unit_match'] == True]
    output_df = output_df.merge(gppd_df, left_on=['plant_id'], right_on=['wepp_id'])

    #drop duplicates after merging gppd_df while keeping the last row. gppd only includes plant info.
    # thus, duplicates point to the same plants. dropping first or last shouldn't make too much of a difference
    output_df.drop_duplicates(subset="entso_unit_id", keep='last', inplace=True)
    return output_df


def load_csv(file_name, directory='./'):
    #function loads csv
    try:
        df = pd.read_csv(os.path.join(directory, file_name), header=0)
        return df
    except IOError as e:
        print(e)


if __name__ == "__main__":
    # read in data
    entso_df = load_csv('entso.csv')
    platts_df = load_csv('platts.csv')
    gppd_df = load_csv('gppd.csv')
    fuel_thesaurus_df = load_csv('fuel_thesaurus.csv')

    # clean dataframes
    entso_df = clean_entso(entso_df)
    platts_df = clean_platts(platts_df)
    gppd_df = clean_gppd(gppd_df)

    # process/transform dataframes
    entso_df = add_fuel_thesaurus(entso_df, fuel_thesaurus_df)
    entso_df = process_entso(entso_df)
    platts_df = process_platts(platts_df)
    gppd_df = process_gppd(gppd_df)

    # merge dataframes
    output_df = merge_power_plant_dfs(entso_df, platts_df, gppd_df)

    #output to csv
    create_csv(output_df, "mapping")