import pandas as pd
import hashlib
import numpy as np
import re


def find_intersection(col_1, col_2):
    return np.intersect1d(col_1, col_2)

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
    input_df['unit_name'] = input_df['unit_name'].str.upper()
    input_df['unit_name'] = input_df['unit_name'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]+', ' ', x))

    # ideally, for the future use a lookup dict for country names if list is available
    platts_country_dict = {'UNITED KINGDOM': 'ENGLAND & WALES'}
    input_df['country_platts'] = input_df['country'].replace(platts_country_dict)

    input_df['plant_name'] = input_df['plant_name'].str.normalize('NFKD').str.encode('ascii',
                                                                                     errors='ignore').str.decode(
        'utf-8')

    return input_df

def clean_platts(input_df):
    input_df['UNIT'] = input_df['UNIT'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]+', ' ', x))
    input_df['PLANT'] = input_df['PLANT'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]+', ' ', x))
    input_df['plant_id'] = input_df['plant_id'].apply(str)

    return input_df

def clean_gppd(input_df):
    input_df['plant_name'] = input_df['plant_name'].str.upper()
    input_df['country_long'] = input_df['country_long'].str.upper()
    input_df['plant_primary_fuel'] = input_df['plant_primary_fuel'].str.upper()
    input_df['plant_name'] = input_df['plant_name'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]+', ' ', x))

    return input_df

#read in data
entso_df = pd.read_csv('entso.csv', header = 0)
platts_df = pd.read_csv('platts.csv', header = 0)
gppd_df = pd.read_csv('gppd.csv', header = 0)
fuel_thesaurus_df = pd.read_csv('fuel_thesaurus.csv', header = 0)


#clean country

  #Would create a country theasarus if there were more countries

entso_df = clean_entso(entso_df)
platts_df = clean_platts(platts_df)



#'plant_name', 'country_long', 'plant_primary_fuel'

#entso_df['country'] = entso_df['country'].apply(lambda x: remove_paren(x))

ping = dict(fuel_thesaurus_df[['unit_fuel_platts_entsoe', 'plant_primary_fuel_gppd']].values)

entso_df['unit_fuel'] = entso_df['unit_fuel'].str.lower()
#platts_df['UNIT_FUEL'] = platts_df['UNIT_FUEL'].str.lower()
entso_df['country'] = entso_df['country'].str.upper()
entso_df['plant_name'] = entso_df['plant_name'].str.upper()
entso_df['plant_name'] = entso_df.apply(lambda row : max(row.plant_name.split(), key=len), axis=1)

platts_df['PLANT'] = platts_df.apply(lambda row : max(row.PLANT.split(), key=len), axis=1)

entso_df = entso_df.merge(fuel_thesaurus_df, left_on='unit_fuel', right_on='unit_fuel_platts_entsoe')
entso_df['plant_primary_fuel_gppd'] = entso_df['plant_primary_fuel_gppd'].str.upper()



print(gppd_df.info())


entso_df = entso_df.drop(['plant_capacity_mw', 'unit_fuel_platts_entsoe', 'note'], axis=1)

print(entso_df.info())
entso_df.rename(columns={'unit_id': 'entso_unit_id'}, inplace=True)
platts_df.rename(columns={ 'unit_id': 'platts_unit_id'}, inplace=True)
gppd_df.rename(columns={ 'plant_id': 'gppd_plant_id'}, inplace=True)
country_intersect_platts = find_intersection(platts_df['COUNTRY'].values, entso_df['country_platts'].values)

platts_df = platts_df.drop(['SUBREGION', 'AREA', 'STATE', 'CITY', 'YEAR'], axis=1)
country_intersect_platts = list(country_intersect_platts)
print(platts_df.info())
platts_df = platts_df[platts_df['COUNTRY'].isin(country_intersect_platts)]

print(platts_df.info())




output_df = entso_df.merge(platts_df, left_on=['plant_name', 'plant_primary_fuel_gppd', 'country'], right_on=['PLANT', 'UNIT_FUEL', 'COUNTRY'], how='inner')



output_df['unit_match'] = output_df.apply(lambda row: max(row.unit_name.split(), key=len) in row.UNIT and
                                                      row.unit_name[-1] == row.UNIT[-1] and
                                                      (row.unit_capacity_mw*.8 < row.UNIT_CAPACITY_MW < row.unit_capacity_mw*1.2),
                                          axis=1)


output_df = output_df[output_df['unit_match'] == True]


output_df = output_df.merge(gppd_df, left_on=['plant_id'], right_on=['wepp_id'])
output_df.drop_duplicates(subset ="entso_unit_id", keep = 'last', inplace = True)







create_csv(output_df, "mapping")