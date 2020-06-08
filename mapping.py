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

#read in data
entso_df = pd.read_csv('entso.csv', header = 0)
platts_df = pd.read_csv('platts.csv', header = 0)
gppd_df = pd.read_csv('gppd.csv', header = 0)
fuel_thesaurus_df = pd.read_csv('fuel_thesaurus.csv', header = 0)


#clean country

platts_country_dict = {'UNITED KINGDOM' : 'ENGLAND & WALES', 'NETHERLANDS' : 'dog'}  #Would create a country theasarus if there were more countries
#entso_df['country'] = entso_df['country'].str.upper()
entso_df['country'] = entso_df['country'].str.split('(').str[0].str.upper().str.strip()
entso_df['unit_name'] = entso_df['unit_name'].str.upper()
entso_df['unit_name'] = entso_df['unit_name'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]+', ' ', x))
platts_df['UNIT'] = platts_df['UNIT'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]+', ' ', x))
platts_df['PLANT'] = platts_df['PLANT'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]+', ' ', x))
entso_df['country_platts'] = entso_df['country'].replace('UNITED KINGDOM', 'ENGLAND & WALES')
entso_df['plant_name'] = entso_df['plant_name'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')


#entso_df['country'] = entso_df['country'].apply(lambda x: remove_paren(x))


#
# print(country_intersection)
# print(len(country_intersection))




#mapping = dict(fuel_thesaurus_df[['unit_fuel_platts_entsoe', 'plant_primary_fuel_gppd']].values)

entso_df['unit_fuel'] = entso_df['unit_fuel'].str.lower()
#platts_df['UNIT_FUEL'] = platts_df['UNIT_FUEL'].str.lower()
entso_df['country'] = entso_df['country'].str.upper()
entso_df['plant_name'] = entso_df['plant_name'].str.upper()
entso_df['plant_name'] = entso_df.apply(lambda row : max(row.plant_name.split(), key=len), axis=1)

platts_df['PLANT'] = platts_df.apply(lambda row : max(row.PLANT.split(), key=len), axis=1)
#replace, or maybe considering using a merge to create a new column
#entso_df['unit_fuel'].replace(mapping, inplace = True)
entso_df = entso_df.merge(fuel_thesaurus_df, left_on='unit_fuel', right_on='unit_fuel_platts_entsoe')
entso_df['plant_primary_fuel_gppd'] = entso_df['plant_primary_fuel_gppd'].str.upper()
#print(entso_df[['unit_capacity_mw', 'unit_fuel','unit_fuel_platts_entsoe']])


print(entso_df[['plant_name', 'plant_primary_fuel_gppd']])
#platts_df['UNIT_FUEL'].replace(mapping, inplace = True)

entso_df = entso_df.drop(['plant_capacity_mw', 'unit_fuel_platts_entsoe', 'note'], axis=1)

print(entso_df.info())

country_intersect_platts = find_intersection(platts_df['COUNTRY'].values, entso_df['country_platts'].values)

platts_df = platts_df.drop(['SUBREGION', 'AREA', 'STATE', 'CITY', 'YEAR'], axis=1)
country_intersect_platts = list(country_intersect_platts)
print(platts_df.info())
#platts_df = platts_df[platts_df['COUNTRY'].isin(country_intersect_platts)]

print(platts_df.info())


#create_hashed_id(entso_df,entso_df,'plant_name', 'country_platts', 'plant_primary_fuel_gppd')
#create_hashed_id(platts_df,platts_df,'PLANT', 'UNIT_FUEL', 'COUNTRY')

# print(entso_df.iloc[0])
# print(platts_df.iloc[27596])

# entso_df = entso_df.reset_index(drop=True)
# platts_df = platts_df.reset_index(drop=True)

output_df = entso_df.merge(platts_df, left_on=['plant_name', 'plant_primary_fuel_gppd', 'country'], right_on=['PLANT', 'UNIT_FUEL', 'COUNTRY'], how='inner')

#output_df = output_df.query("unit_name == UNIT")
output_df['unit_match'] = output_df.apply(lambda row: max(row.unit_name.split(), key=len) in row.UNIT and
                                                      row.unit_name[-1] == row.UNIT[-1] and
                                                      (row.unit_capacity_mw*.8 < row.UNIT_CAPACITY_MW < row.unit_capacity_mw*1.2),
                                          axis=1)
output_df = output_df[output_df['unit_match'] == True]
#output_df = output_df[['unit_id_x', 'unit_id_y']]
output_df.to_csv("output.csv")

print(output_df.head(5))

output_diff = find_diff(output_df['unit_id_x'].values, entso_df['unit_id'].values)
print(list(output_diff))
#print(output_df[['plant_primary_fuel_gppd', 'country_platts', 'unit_name']])


# df = pd.DataFrame()
# hashed_values_w_unit = []
# hashed_values = []
# hashed_values_w_unit = df_entso.apply(lambda row: hash(tuple(row)), axis = 1)
# hashed_values = df_entso[['unit_fuel', 'country', 'plant_name']].apply(lambda row: hash(tuple(row)), axis = 1)
# df_entso['hashed_val_unit'] = hashed_values_w_unit
# df_entso['hashed_val'] = hashed_values
# print(df_entso.head(5))
#
#




#print(entso_df.hashed_id)

#create instersections before processing data, use np vectorization .values
#then use subsetting



#platts_country = set(platts_df.COUNTRY.unique())

#entso_country  = set(entso_df.country.unique())
#
# print(len(entso_country))
# print(len(platts_country))
#
# print(len(platts_country.intersection(entso_country)))
#


#use merge if using multicolum
#use join if using index