import pandas as pd
import database_processing as dbp
import backend.address_processing as ctl
import os
import uuid
os.chdir(os.path.dirname(os.path.abspath(__file__)))

def preload_directory_address_data(filename, file_type):
    if file_type == 'csv':
        address_df = pd.read_csv(filename).astype(str)
    elif file_type == 'xlsx':
        address_df = pd.read_excel(filename).astype(str)
    else:
        return {'file_contents': None, 'errors': ["Unsupported MIME type"]}
    return address_df


if __name__ == "__main__":
    addr_df = preload_directory_address_data('../../fs-brightpath-mock-data/testing/Marin_addresses_street_city_state_postal.csv', 'csv')
    #print(addr_df.head())

    addr_df['address_list'] = addr_df['street_address'] + ', ' + addr_df['city'] + ', ' + addr_df['state'] + ', ' + addr_df['postalcode']
    addr_df['census_tract_data'] = addr_df.apply(lambda row: ctl.run_census_tract_lookup(row['address_list']), axis=1)
    addr_df['census_tract_id'] = addr_df['census_tract_data'].apply(lambda x: x.get('census_tract_id') if isinstance(x, dict) else None)
    addr_df['census_tract_lat'] = addr_df['census_tract_data'].apply(lambda x: x.get('census_tract_lat') if isinstance(x, dict) else None)
    addr_df['census_tract_lon'] = addr_df['census_tract_data'].apply(lambda x: x.get('census_tract_lon') if isinstance(x, dict) else None)
    addr_df['respondent_address_id'] = [uuid.uuid4() for _ in range(len(addr_df.index))]
    addr_insert_df = addr_df[['respondent_address_id','street_address', 'city', 'state', 'postalcode', 'census_tract_id', 'census_tract_lat', 'census_tract_lon']]
    #addr_df['census_api_called'] = addr_df['census_tract_data'].apply(lambda x: True if isinstance(x, dict) else False)
    addr_insert_df.to_sql('respondent_address', dbp.create_database_engine(), if_exists='append', schema='directory',index=False)
    print(addr_insert_df.head())