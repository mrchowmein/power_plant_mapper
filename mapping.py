import pandas as pd
import numpy as np
import re


def filter_by_intersection(df_to_filter, col_to_filter, df_col_to_filter, def2_col):
    intersection = list(np.intersect1d(df_col_to_filter, def2_col))
    df_to_filter = df_to_filter[df_to_filter[f'{col_to_filter}'].isin(intersection)]

    return df_to_filter

def find_diff(col1, col2):
    return np.setxor1d(col1, col2)


def create_hashed_id(sourcedf,destinationdf,*column):
    columnName = ''
    destinationdf['hashed_id'] = pd.DataFrame(sourcedf[list(column)].values.sum(axis=1))[0].str.encode('utf-8').apply(lambda x: (hashlib.sha256(x).hexdigest().upper()))


def create_csv(output_df, file_name, all_columns=False):
    if not all_columns:
        output_df = output_df[['entso_unit_id', 'platts_unit_id', 'gppd_plant_id']]
    output_df.to_csv(f"{file_name}.csv", index=False)


def clean_entso(input_df):
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

    return input_df

def clean_platts(input_df):
    input_df['UNIT'] = input_df['UNIT'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]+', ' ', x))
    input_df['PLANT'] = input_df['PLANT'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]+', ' ', x))
    input_df['plant_id'] = input_df['plant_id'].apply(str)
    input_df['PLANT'] = input_df.apply(lambda row: max(row.PLANT.split(), key=len), axis=1)

    return input_df


def clean_gppd(input_df):
    input_df['plant_name'] = input_df['plant_name'].str.upper()
    input_df['country_long'] = input_df['country_long'].str.upper()
    input_df['plant_primary_fuel'] = input_df['plant_primary_fuel'].str.upper()
    input_df['plant_name'] = input_df['plant_name'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]+', ' ', x))

    return input_df


def add_fuel_thesaurus(input_df, fuel_thesaurus_df):
    input_df = input_df.merge(fuel_thesaurus_df, left_on='unit_fuel', right_on='unit_fuel_platts_entsoe')
    input_df['plant_primary_fuel_gppd'] = input_df['plant_primary_fuel_gppd'].str.upper()

    return input_df

def process_entso(input_df):
    input_df['unit_name'] = input_df['unit_name'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]+', ' ', x))
    input_df['plant_name'] = input_df.apply(lambda row: max(row.plant_name.split(), key=len), axis=1)
    input_df = input_df.drop(['plant_capacity_mw', 'unit_fuel_platts_entsoe', 'note'], axis=1)
    input_df.rename(columns={'unit_id': 'entso_unit_id'}, inplace=True)

    return input_df


def process_platts(input_df):
    input_df.rename(columns={'unit_id': 'platts_unit_id'}, inplace=True)
    input_df = input_df.drop(['SUBREGION', 'AREA', 'STATE', 'CITY', 'YEAR'], axis=1)

    return input_df

def process_gppd(input_df):
    gppd_df.rename(columns={'plant_id': 'gppd_plant_id'}, inplace=True)

    return gppd_df

def merge_power_plant_dfs(entso_df, platts_df, gppd_df):
    output_df = entso_df.merge(platts_df, left_on=['plant_name', 'plant_primary_fuel_gppd', 'country'],
                               right_on=['PLANT', 'UNIT_FUEL', 'COUNTRY'], how='inner')

    output_df['unit_match'] = output_df.apply(lambda row: max(row.unit_name.split(), key=len) in row.UNIT and
                                            row.unit_name[-1] == row.UNIT[-1] and
                                            (row.unit_capacity_mw * .8 < row.UNIT_CAPACITY_MW < row.unit_capacity_mw * 1.2),
                                            axis=1)

    output_df = output_df[output_df['unit_match'] == True]
    output_df = output_df.merge(gppd_df, left_on=['plant_id'], right_on=['wepp_id'])
    output_df.drop_duplicates(subset="entso_unit_id", keep='last', inplace=True)
    return output_df

#read in data
entso_df = pd.read_csv('entso.csv', header = 0)
platts_df = pd.read_csv('platts.csv', header = 0)
gppd_df = pd.read_csv('gppd.csv', header = 0)
fuel_thesaurus_df = pd.read_csv('fuel_thesaurus.csv', header = 0)


#clean dataframes
entso_df = clean_entso(entso_df)
platts_df = clean_platts(platts_df)
gppd_df = clean_gppd(gppd_df)

#process/transform dataframes
entso_df = add_fuel_thesaurus(entso_df, fuel_thesaurus_df)
entso_df = process_entso(entso_df)
platts_df = process_platts(platts_df)
gppd_df = process_gppd(gppd_df)

platts_df = filter_by_intersection(platts_df, 'COUNTRY', platts_df['COUNTRY'].values, entso_df['country_platts'].values)

print(gppd_df.head(4))
output_df = merge_power_plant_dfs(entso_df, platts_df, gppd_df)


create_csv(output_df, "mapping")