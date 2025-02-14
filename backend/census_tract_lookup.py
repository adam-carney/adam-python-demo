import requests
import database_processing as dbp
from fuzzywuzzy import fuzz
from sqlalchemy import create_engine, Table, MetaData, text, bindparam, update
from sqlalchemy.dialects.postgresql import insert
import json

DIRECTORY_DF = dbp.get_respondent_addresses()
DIRECTORY_DF['full_address'] = DIRECTORY_DF['street_address'] + ', ' + DIRECTORY_DF['city'] + ', ' + DIRECTORY_DF['state'] + ', ' + DIRECTORY_DF['postalcode']
CENSUS_TRACT_DATA_COLUMN_LIST = ['asq_test_filename', 'respondent_id', 'respondent_address_id', 'asq_address_processed', 'directory_address_matched']
CENSUS_TRACT_PROCESSING_LIST = CENSUS_TRACT_DATA_COLUMN_LIST.__add__(['census_api_called', 'census_api_result'])


def get_census_tract_data(street, city, state, postalcode, benchmark=4, vintage=4, layers="Census+Tracts"):
    base_url = "https://geocoding.geo.census.gov/geocoder/geographies/address"

    params = {
        "street": street,
        "city": city,
        "state": state,
        "zip": postalcode,
        "benchmark": benchmark,
        "vintage": vintage,
        "format": "json",
        "layers": layers
    }
    response = requests.get(base_url, params=params).json()
    
    # Example usage:
    
    matched_address_length = len(response['result']['addressMatches'])
    result_dict = {}
    if matched_address_length == 1:
        input_address = response['result']['addressMatches'][0]['matchedAddress']
        geographies = response['result']['addressMatches'][0]['geographies']['Census Tracts']
        for tract in geographies:
            result_dict = {
                "street": street,
                "city": city,
                "state": state,
                "matched_address": input_address,
                "census_tract_id": tract['GEOID'],
                "census_tract_lat": tract['CENTLAT'],
                "census_tract_lon": tract['CENTLON']
            }
    print(f" Received the following result from the Census Tract API: {result_dict}")
    return result_dict

def update_directory_with_census_data(df):
    try:
        engine = dbp.create_database_engine()
        connection = engine.connect()
        metadata = MetaData()
        respondent_address = Table('respondent_address', metadata, autoload_with=engine, schema='directory')

        for index, row in df[df['census_api_called'] == True].iterrows():
            stmt = (update(respondent_address)
                    .where(respondent_address.c.respondent_address_id == row['respondent_address_id'])
                    .values(census_tract_id=row['census_tract_id'],
                            census_tract_lat=row['census_tract_lat'],
                            census_tract_lon=row['census_tract_lon']))
            connection.execute(stmt)
            connection.commit()
        connection.close()
        print("Updated directory with census data")
    except Exception as e:
        print(f"Error updating directory with census data: {str(e)}")
        return None

def insert_census_tract_processing(df):
    try:
        engine = dbp.create_database_engine()
        connection = engine.connect()
        metadata = MetaData()
        census_tract_processing = Table('census_tract_processing', metadata, autoload_with=engine, schema='asq')
        df['census_api_result'] = df['census_api_result'].fillna(0  )
        insert_data = df[CENSUS_TRACT_PROCESSING_LIST].to_dict(orient='records')
        stmt = insert(census_tract_processing).values(insert_data)
        connection.execute(stmt)
        connection.commit()
        connection.close()
    except Exception as e:
        print(f"Error inserting census tract processing data: {str(e)}")
        return None



def run_census_tract_lookup(address_data):
    try:
        addr_split = address_data.split(',')
        street_addr = addr_split[0]
        city = addr_split[1]
        state = addr_split[2]
        postalcocde = addr_split[3]
        print (f"Running census tract lookup for {street_addr}, {city}, {state}, {postalcocde}")
        census_tract_data = get_census_tract_data(street_addr, city, state, postalcocde)
        if len(census_tract_data) == 0:
            census_tract_data = {
                "street": street_addr,
                "city": city,
                "state": state,
                "matched_address": None,
                "census_tract_id": None,
                "census_tract_lat": None,
                "census_tract_lon": None
            }   
        return census_tract_data
    except Exception as e:
        print(f"Error running census tract lookup: {str(e)}")
        return None


def update_asq_test_details_addr_id(df):
    try:
        
        df_items = ['asq_test_filename', 'respondent_id', 'respondent_address_id']
        update_data = df[df_items].to_dict(orient='records')
        engine = dbp.create_database_engine()
        connection = engine.connect()
        metadata = MetaData()
        asq_test_details = Table('asq_test_details', metadata, autoload_with=engine, schema='asq', include_columns=['asq_test_filename', 'respondent_id', 'respondent_address_id'])
        for row in update_data:
            stmt = (update(asq_test_details).where(asq_test_details.c.asq_test_filename == row['asq_test_filename']).where(asq_test_details.c.respondent_id == row['respondent_id']).values(respondent_address_id=row['respondent_address_id']))
            connection.execute(stmt)
            connection.commit()
        connection.close()
    except Exception as e:
        print(f"Error upserting ASQ test details: {str(e)}")
        return None

def check_census_tract_exists(respondent_address_id):
    try:

        census_tract_id = DIRECTORY_DF.loc[DIRECTORY_DF['respondent_address_id'] == respondent_address_id, 'census_tract_id'].values[0]
        return census_tract_id
    except Exception as e:
        print(f"Error checking census tract exists: {str(e)}")
        return None

def check_census_tract_lat_exists(respondent_address_id):
    try:
        census_tract_lat = DIRECTORY_DF.loc[DIRECTORY_DF['respondent_address_id'] == respondent_address_id, 'census_tract_lat'].values[0]
        return census_tract_lat
    except Exception as e:
        print(f"Error checking census tract lat exists: {str(e)}")
        return None

def check_census_tract_lon_exists(respondent_address_id):
    try:
        census_tract_lon = DIRECTORY_DF.loc[DIRECTORY_DF['respondent_address_id'] == respondent_address_id, 'census_tract_lon'].values[0]
        return census_tract_lon
    except Exception as e:
        print(f"Error checking census tract lon exists: {str(e)}")
        return None

def fuzzy_match_addresses(concatenated_address):
    best_match_id = None
    highest_ratio = 0

    for index, row in DIRECTORY_DF.iterrows():
        ratio = fuzz.ratio(row['full_address'], concatenated_address)
        if ratio > highest_ratio:
            highest_ratio = ratio
            best_match_id = row['respondent_address_id'] + '|' + row['full_address']

    if highest_ratio < 85:
        return None
    else:
        return best_match_id

def process_address_data(asq_filename):
    try:
        
        asq_test_df = dbp.get_respondent_address_by_filename(asq_filename)
        asq_test_df['asq_test_filename'] = asq_filename
        asq_test_df.rename(columns={'respondent_address_id': 'asq_respondent_addr_id'}, inplace=True)
        asq_test_df['asq_address_processed'] = asq_test_df['street_address'] + ', ' + asq_test_df['city'] + ', ' + asq_test_df['state'] + ', ' + asq_test_df['postalcode']
        asq_test_df['matched_directory_address_data'] = asq_test_df['asq_address_processed'].apply(fuzzy_match_addresses)
        asq_test_df['respondent_address_id'] = asq_test_df['matched_directory_address_data'].str.split('|').str[0]
        asq_test_df['directory_address_matched'] = asq_test_df['matched_directory_address_data'].str.split('|').str[1]
        asq_address_df = asq_test_df[CENSUS_TRACT_DATA_COLUMN_LIST]
        update_asq_test_details_addr_id(asq_address_df[asq_address_df['respondent_address_id'].notnull()])
        return asq_address_df
    except Exception as e:
        print(f"Error processing address data: {str(e)}")
        return None

def process_census_tract_data(asq_filename):
    try:
        asq_address_df = process_address_data(asq_filename)
        asq_address_df['census_tract_id'] = asq_address_df['respondent_address_id'].apply(check_census_tract_exists)
        asq_address_df['census_tract_lat'] = asq_address_df['respondent_address_id'].apply(check_census_tract_lat_exists)
        asq_address_df['census_tract_lon'] = asq_address_df['respondent_address_id'].apply(check_census_tract_lon_exists)
        asq_address_df.loc[asq_address_df['census_tract_id'].isnull(), 'census_api_result'] = asq_address_df.loc[asq_address_df['census_tract_id'].isnull(), 'asq_address_processed'].apply(run_census_tract_lookup)
        asq_address_df['census_tract_id'] = asq_address_df['census_api_result'].apply(lambda x: x.get('census_tract_id') if isinstance(x, dict) else None)
        asq_address_df['census_tract_lat'] = asq_address_df['census_api_result'].apply(lambda x: x.get('census_tract_lat') if isinstance(x, dict) else None)
        asq_address_df['census_tract_lon'] = asq_address_df['census_api_result'].apply(lambda x: x.get('census_tract_lon') if isinstance(x, dict) else None)
        asq_address_df['census_api_called'] = asq_address_df['census_api_result'].apply(lambda x: True if isinstance(x, dict) else False)
        insert_census_tract_processing(asq_address_df)
        update_directory_with_census_data(asq_address_df)
        return asq_address_df
    except Exception as e:
        print(f"Error processing census tract data: {str(e)}")
        return None


if __name__ == "__main__":
    print(process_census_tract_data("mock_respondent_data_20241231160343_7d292c3c.xlsx"))
    census_tract_data = get_census_tract_data("20 E Milwaukee", "Janesville", "WI", "")
    print(census_tract_data.get('census_tract_id'))
