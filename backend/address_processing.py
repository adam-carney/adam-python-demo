import requests
import database_processing as dbp
from rapidfuzz import fuzz
from sqlalchemy import create_engine, Table, MetaData, text, bindparam, update
from sqlalchemy.dialects.postgresql import insert
import json
import os
import datetime
from splink import DuckDBAPI, block_on, Linker, SettingsCreator

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Change the current working directory to the script directory
os.chdir(script_dir)
DIRECTORY_DF = dbp.get_respondent_addresses()
DIRECTORY_DF['full_address'] = DIRECTORY_DF['street_address'] + ', ' + DIRECTORY_DF['city'] + ', ' + DIRECTORY_DF['state'] + ', ' + DIRECTORY_DF['postalcode']
CENSUS_TRACT_DATA_COLUMN_LIST = ['asq_test_filename', 'respondent_id', 'respondent_address_id', 'asq_address_processed', 'directory_address_matched', 'census_tract_id', 'census_tract_lat', 'census_tract_lon']
CENSUS_TRACT_PROCESSING_LIST = ['asq_test_filename', 'respondent_id', 'respondent_address_id', 'asq_address_processed', 'directory_address_matched','census_api_called', 'census_api_result']


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
        df['census_api_result'] = df['census_api_result'].fillna(0)
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

def load_model_and_predict(asq_df):
    return_list = ['asq_respondent_addr_id', 'respondent_id', 'asq_test_filename', 'asq_address_processed',
       'matched_directory_address_data', 'census_tract_id', 'census_tract_lat','census_tract_lon']
    model_path = 'addr_matching.json'
    match_probability=0.9799999999
    db_api = DuckDBAPI()
    df1 =asq_df[['asq_respondent_addr_id', 'street_address', 'city', 'state', 'postalcode']].rename(columns={'asq_respondent_addr_id': 'unique_id'})
    df2 = DIRECTORY_DF[['respondent_address_id', 'street_address', 'city', 'state', 'postalcode']].rename(columns={'respondent_address_id': 'unique_id'})
    # Load the model
    linker = Linker([df1, df2], model_path, db_api=db_api, input_table_aliases=["asq_data", "directory_data"])
    
    # Run prediction
    try:
        df_predict = linker.inference.predict(threshold_match_probability=match_probability)
        df_result = df_predict.as_pandas_dataframe()
        asq_data_df = df_result.merge(asq_df, left_on='unique_id_l', right_on='asq_respondent_addr_id', how='left')
        directory_data_df = asq_data_df.merge(DIRECTORY_DF, left_on='unique_id_r', right_on='respondent_address_id', how='left')
        directory_data_df['matched_directory_address_data'] = directory_data_df['respondent_address_id'] + '|' + directory_data_df['full_address']
        return_data = directory_data_df[return_list]
        return return_data
    except Exception as e:
        print(f'Prediction failed: {e}')
        return_data['matched_directory_address_data']= None
        return return_data



def process_address_data(asq_filename):
    try:
        print (f"Processing address data for {asq_filename}")
        pad_start_time = datetime.datetime.now()
        asq_test_df = dbp.get_respondent_address_by_filename(asq_filename)
        asq_test_df['asq_test_filename'] = asq_filename
        asq_test_df.rename(columns={'respondent_address_id': 'asq_respondent_addr_id'}, inplace=True)
        asq_test_df['asq_address_processed'] = asq_test_df['street_address'] + ', ' + asq_test_df['city'] + ', ' + asq_test_df['state'] + ', ' + asq_test_df['postalcode']
        asq_data_df = load_model_and_predict(asq_test_df)
        asq_data_df['respondent_address_id'] = asq_data_df['matched_directory_address_data'].str.split('|').str[0]
        asq_data_df['directory_address_matched'] = asq_data_df['matched_directory_address_data'].str.split('|').str[1]
        asq_address_df = asq_data_df[CENSUS_TRACT_DATA_COLUMN_LIST]
        update_asq_test_details_addr_id(asq_address_df[(asq_address_df['respondent_address_id'].notnull()) & (asq_address_df['asq_address_processed'] != asq_address_df['directory_address_matched'])])
        pad_end_time = datetime.datetime.now()
        print(f"Processed address data for {asq_filename} in {pad_end_time - pad_start_time}")
        print(f"Processed address data for {asq_filename}")
        return asq_address_df
    except Exception as e:
        print(f"Error processing address data: {str(e)}")
        return None

def process_census_tract_data(asq_filename):
    try:
        start_time = datetime.datetime.now()
        asq_address_df = process_address_data(asq_filename)
        asq_address_df.loc[asq_address_df['census_tract_id'].isnull(), 'census_api_result'] = asq_address_df.loc[asq_address_df['census_tract_id'].isnull(), 'asq_address_processed'].apply(run_census_tract_lookup)
        asq_address_df['census_tract_id'] = asq_address_df['census_api_result'].apply(lambda x: x.get('census_tract_id') if isinstance(x, dict) else None)
        asq_address_df['census_tract_lat'] = asq_address_df['census_api_result'].apply(lambda x: x.get('census_tract_lat') if isinstance(x, dict) else None)
        asq_address_df['census_tract_lon'] = asq_address_df['census_api_result'].apply(lambda x: x.get('census_tract_lon') if isinstance(x, dict) else None)
        asq_address_df['census_api_called'] = asq_address_df['census_api_result'].apply(lambda x: True if isinstance(x, dict) else False)
        insert_census_tract_processing(asq_address_df)
        update_directory_with_census_data(asq_address_df)
        end_time = datetime.datetime.now()
        print(f"Processed census tract data for {asq_filename} in {end_time - start_time}")
        return asq_address_df
    except Exception as e:
        print(f"Error processing census tract data: {str(e)}")
        return None


if __name__ == "__main__":
    print(process_census_tract_data("mock_respondent_data_upload_20250114221220_88f04c38.xlsx"))
