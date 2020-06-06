import pandas as pd
import hashlib
import timeit

def remove_paren(country):
    return country.split('(')[0].strip()


entso_df = pd.read_csv('entso.csv', header = 0)
platts_df = pd.read_csv('platts.csv', header = 0)

gppd_df = pd.read_csv('gppd.csv', header = 0)

#clean country

entso_df['country'] = entso_df['country'].apply(lambda x: remove_paren(x))


fuel_thesaurus_df = pd.read_csv('fuel_thesaurus.csv', header = 0)
mapping = dict(fuel_thesaurus_df[['unit_fuel_platts_entsoe', 'plant_primary_fuel_gppd']].values)

entso_df['unit_fuel'] = entso_df['unit_fuel'].str.lower()
platts_df['UNIT_FUEL'] = platts_df['UNIT_FUEL'].str.lower()

entso_df['unit_fuel'].replace(mapping, inplace = True)
platts_df['UNIT_FUEL'].replace(mapping, inplace = True)


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
def create_hashed_id(sourcedf,destinationdf,*column):
    columnName = ''
    destinationdf['hashed_id'] = pd.DataFrame(sourcedf[list(column)].values.sum(axis=1))[0].str.encode('utf-8').apply(lambda x: (hashlib.sha256(x).hexdigest().upper()))


create_hashed_id(entso_df,entso_df,'unit_fuel', 'country', 'unit_name')
print(entso_df.hashed_id)



