from sqlalchemy import create_engine, Table, Column, String, Date, MetaData, TIMESTAMP, insert, ForeignKey, Integer, Boolean
from sqlalchemy.dialects.postgresql import VARCHAR, insert
from datetime import datetime
from sqlalchemy import update
import pandas as pd
import database_processing as dbp
import address_processing as ap

metadata = MetaData(schema='directory')
    # List of columns from directory.respondent
RESPONDENT_COLUMNS = [
        'respondent_id', 'first_name', 'middle_name', 'last_name', 'birthdate',
        'gender', 'race_ethnicity'
    ]

RESPONDENT_ADDR_MAP_COLUMNS = [
        'respondent_address_id', 'respondent_id'
    ]

RESPONDENT_ADDR_COLUMNS = [
        'respondent_address_id', 'street_address', 'city', 'state', 'postalcode'
    ]

    # List of columns from directory.asq_test_data
ASQ_TEST_DATA_COLUMNS = [
        'asq_test_filename', 'premature', 'test_date', 'test_type', 'test_interval', 'program',
        'agency', 'source', 'origin', 'test_location', 'proxy_first_name', 'proxy_last_name',
        'proxy_relationship', 'communication_score', 'communication_outcome',
        'communication_recommendation', 'gross_motor_score', 'gross_motor_outcome',
        'gross_motor_recommendation', 'fine_motor_score', 'fine_motor_outcome',
        'fine_motor_recommendation', 'problem_solving_score', 'problem_solving_outcome',
        'problem_solving_recommendation', 'personal_social_score', 'personal_social_outcome',
        'personal_social_recommendation', 'overall_test_score', 'referral', 'referral_concern',
        'referral_agency', 'referral_followup_date', 'referral_source', 'referral_result',
        'test_language','respondent_id'
    ]
# Define the respondent table
respondent = Table('respondent', metadata,autoload_with=dbp.create_database_engine())


# Define the respondent address table
respondent_address_map = Table('respondent_address_map', metadata,autoload_with=dbp.create_database_engine(), include_columns=RESPONDENT_ADDR_MAP_COLUMNS)

# Define the respondent address table
respondent_address = Table('respondent_address', metadata,autoload_with=dbp.create_database_engine())
# Define the asq_test_data table
asq_test_data = Table('asq_test_data', metadata,autoload_with=dbp.create_database_engine())

# Define a function to insert a new asq_test_data record
def insert_asq_test_data(data):
    """
    Insert a new record into the directory.asq_test_data table.

    :param data: A dictionary containing the asq_test_data.
    """
    engine = dbp.create_database_engine()
    with engine.connect() as connection:
        stmt = insert(asq_test_data).values(data)
        connection.execute(stmt)
        connection.commit()
        connection.close()

def insert_respondent_address_map(data):
    """
    Insert a new record into the directory.asq_test_data table.

    :param data: A dictionary containing the asq_test_data.
    """
    engine = dbp.create_database_engine()
    with engine.connect() as connection:
        stmt = insert(respondent_address_map).values(data)
        connection.execute(stmt)
        connection.commit()
        connection.close()

def insert_respondent_address(data):
    """
    Insert a new record into the directory.asq_test_data table.

    :param data: A dictionary containing the asq_test_data.
    """
    engine = dbp.create_database_engine()
    with engine.connect() as connection:
        stmt = insert(respondent_address).values(data)
        connection.execute(stmt)
        connection.commit()
        connection.close()

# Define a function to insert a new respondent
def insert_respondent(data):
    """
    Insert a new respondent into the directory.respondent table.

    :param data: A dictionary containing the respondent data.
    """
    engine = dbp.create_database_engine()
    with engine.connect() as connection:
        stmt = insert(respondent).values(data)
        connection.execute(stmt)
        connection.commit()
        connection.close()


def process_new_respondent(new_respondent):
    """
    Process a new respondent and insert it into the directory schema.

    :param new_respondent: A dictionary containing the new respondent data.
    """
    respondent_data = new_respondent[RESPONDENT_COLUMNS].drop_duplicates().to_dict(orient='records')
    respondent_address_data = new_respondent[RESPONDENT_ADDR_COLUMNS].drop_duplicates().to_dict(orient='records')
    respondent_address_map_data = new_respondent[RESPONDENT_ADDR_MAP_COLUMNS].to_dict(orient='records')
    insert_respondent(respondent_data)
    insert_respondent_address(respondent_address_data)
    insert_respondent_address_map(respondent_address_map_data)

    insert_asq_test_data(new_respondent[ASQ_TEST_DATA_COLUMNS].to_dict(orient='records'))

def process_updated_respondent(updated_respondent):
    """
    Process an updated respondent and update it in the directory schema.

    :param updated_respondent: A dictionary containing the updated respondent data.
    """
    asq_test_data = updated_respondent[ASQ_TEST_DATA_COLUMNS].to_dict(orient='records')
    updated_respondent_data = updated_respondent[RESPONDENT_COLUMNS].drop_duplicates().to_dict(orient='records')[0]
    updated_respondent_address_map = updated_respondent[RESPONDENT_ADDR_MAP_COLUMNS].drop_duplicates().to_dict(orient='records')[0]
    updated_respondent_address = updated_respondent[RESPONDENT_ADDR_COLUMNS].drop_duplicates().to_dict(orient='records')[0]
    engine = dbp.create_database_engine()
    connection = engine.connect()
    # Perform upsert for respondent data
    stmt = insert(respondent).values(updated_respondent_data)
    stmt = stmt.on_conflict_do_update(
        index_elements=['respondent_id'],
        set_={c.key: c for c in stmt.excluded if c.key != 'respondent_id'}
    )
    with engine.connect() as connection: 
        result = connection.execute(stmt)
        connection.commit()

    # Perform upsert for respondent address data
    stmt = insert(respondent_address).values(updated_respondent_address)
    stmt = stmt.on_conflict_do_update(
        index_elements=['respondent_address_id'],
        set_={c.key: c for c in stmt.excluded if c.key != 'respondent_address_id'}
    )
    with engine.connect() as connection: 
        result = connection.execute(stmt)
        connection.commit()

    # Perform upsert for respondent address map data
    stmt = insert(respondent_address_map).values(updated_respondent_address_map)
    update_dict = {c.key: c for c in stmt.excluded if c.key not in ['respondent_id', 'respondent_address_id']}
    if update_dict:
        stmt = stmt.on_conflict_do_update(
            index_elements=['respondent_id', 'respondent_address_id'],
            set_=update_dict
        )
        with engine.connect() as connection:
            connection.execute(stmt)
            connection.commit()

    insert_asq_test_data(asq_test_data)

def process_partial_match_respondent(partial_match_respondent):
    """
    Process a partial match respondent and update it in the directory schema.

    :param partial_match_respondent: A dictionary containing the partial match respondent data.
    """
    insert_asq_test_data(partial_match_respondent[ASQ_TEST_DATA_COLUMNS].to_dict(orient='records'))

def process_asq_data_updates(asq_test_filename):
    """
    Process ASQ data and insert/update it into the directory schema.

    :param asq_test_filename: Filename that is being processed
    """
    engine = dbp.create_database_engine()
    try:
        filename_test_records = pd.read_sql(f"""select asq_test_filename , first_name, middle_name, last_name, birthdate, gender, race_ethnicity, street_address, city, state, postalcode,
            premature, test_date, test_type, test_interval, "program", agency, "source", origin, test_location, proxy_first_name, proxy_last_name, proxy_relationship,
            communication_score, communication_outcome, communication_recommendation, gross_motor_score, gross_motor_outcome, gross_motor_recommendation, fine_motor_score,
            fine_motor_outcome, fine_motor_recommendation, problem_solving_score, problem_solving_outcome, problem_solving_recommendation, personal_social_score,
            personal_social_outcome, personal_social_recommendation, overall_test_score, referral, referral_concern, referral_agency, referral_followup_date, referral_source,
            referral_result, test_language,
            case when upper(atd.respondent_match_status) IN ('DIRECTORY_WRONG', 'PARTIAL_MATCH')
            then (select distinct unique_id_l from asq.asq_directory_matching_summary adms where adms.asq_test_filename = atd.asq_test_filename and adms.unique_id_r = atd.respondent_id)
            else atd.respondent_id
            end as respondent_id, respondent_address_id,upper(respondent_match_status) as respondent_match_status
            FROM asq.asq_test_details atd
            where atd.respondent_match_status != 'SKIP' and atd.asq_test_filename = '{asq_test_filename}'""" , engine)

        new_records = filename_test_records[filename_test_records['respondent_match_status'] == 'NEW']
        updated_records = filename_test_records[filename_test_records['respondent_match_status'] == 'DIRECTORY_WRONG']
        add_test_data = filename_test_records[filename_test_records['respondent_match_status'] == 'PARTIAL_MATCH']
        add_matched_data = filename_test_records[filename_test_records['respondent_match_status'] == 'MATCHED']

        #if len(new_records) > 0:
        #    process_new_respondent(new_records)
        if len(updated_records) > 0:
            process_updated_respondent(updated_records)
        if len(add_test_data) > 0:
            process_partial_match_respondent(add_test_data)
        if len(add_matched_data) > 0:
            process_partial_match_respondent(add_matched_data)
        if len(new_records) > 0:
            process_new_respondent(new_records)
        ap.process_census_tract_data(asq_test_filename)
        
    except Exception as e:
        print(f"Error processing ASQ data updates: {str(e)}")



if __name__ == '__main__':
    process_asq_data_updates('mock_respondent_data_upload_20250209101958_6a37ab40.xlsx')