
import os
from io import BytesIO
import json
import pandas as pd
from dotenv import load_dotenv
from fuzzywuzzy import fuzz
from sqlalchemy import create_engine, text, update
from sqlalchemy.dialects import postgresql
import match_processing
import process_dml_functions as dml
import database_processing as dbp
from datetime import datetime
import asq_score_outcome as aso

date_columns = ['birthdate', 'test_date', 'referral_followup_date']
schema_headers = dbp.fetch_schema_headers_from_db()


def return_dataframe_from_blob(blob_to_process, content_type):
    try:
        print (f'Content type: {content_type}')
        if content_type == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
            df = pd.read_excel(BytesIO(blob_to_process), engine='openpyxl')
        elif content_type == 'application/vnd.ms-excel':
            df = pd.read_excel(BytesIO(blob_to_process), sheet_name=0)  # Assuming the first sheet is desired
        elif content_type == 'text/csv':
            df = pd.read_csv(BytesIO(blob_to_process))
        elif content_type == 'application/json':
            data_dict = json.loads(BytesIO(blob_to_process).decode('utf-8'))
            df = pd.DataFrame(data_dict)
        else:
            return {'file_contents': None, 'errors': ["Unsupported MIME type"]}
        return df
    except Exception as e:
        return {'file_contents': None, 'errors': [f"Error processing file: {str(e)}"]}

def apply_matching(row):

    is_match, schema_header = match_with_fuzzywuzzy(row['index'], schema_headers)
    row['is_match'] = is_match
    row['schema_header_value'] = schema_header
    return row

def match_with_fuzzywuzzy(cell_value, comparison_list):
    test_cell_value = cell_value.replace('_', '').replace(' ', '').replace('-', '').lower()
    for item in comparison_list:
        test_item = item.replace('_', '').lower()
        if fuzz.ratio(test_cell_value, test_item) > 85:  # Adjust threshold as needed
            # print(f'FuzzyWuzzy matched cell value: {cell_value} as "{test_cell_value}" with item: {item} as "{test_item}" with score {fuzz.ratio(cell_value, item)}')
            return True, item
    # print(f'FuzzyWuzzy did not match {cell_value}')
    return False, 'unknown'



def process_dataframe(df):

    df = df.head(3)
    df_transposed = df.transpose()
    df_transposed.reset_index(inplace=True)
    df_transposed['is_match'] = False
    df_transposed['schema_header_value'] = 'unknown'
    df_transposed = df_transposed.apply(apply_matching, axis=1)
    df_transposed = df_transposed.where(pd.notnull(df_transposed), None)
    df_transposed.drop(columns=['last_name_status', 'birthdate_status'], errors='ignore', inplace=True)
    transposed_data = df_transposed.to_dict(orient='records')
    print (f'Transposed data: {transposed_data}')

    return transposed_data

def handle_file(downloaded_blob, content_type):
    return_dict = {
        'file_contents': None,
        'errors': [],
    }

    try:
        start_time = datetime.now()
        print(f'Processing file start: {start_time}')
        df = return_dataframe_from_blob(downloaded_blob, content_type)
        end_time = datetime.now()
        print(f'Processing file end: {end_time}')
        print(f'Processed file: {df}')
        return_dict['file_contents'] = process_dataframe(df)
        print(f'Processed file: {return_dict}')
        return return_dict
    except Exception as e:
        return {'file_contents': None, 'errors': [f"Error processing file: {str(e)}"]}

def prepare_df_for_insert(df, rename_dict, file_name):
    try:
        # Rename the columns based on the mapping
        df.rename(columns=rename_dict, inplace=True)

        # Remove columns that have no header(index)
        df = df.loc[:, df.columns.notna()]

        # Add a new column "asq_test_filename" and set its value to file_name
        df['asq_test_filename'] = file_name

        # Replace NaN values and 'nan' strings with None
        df = df.apply(lambda x: x.map(lambda y: None if pd.isnull(y) or y == 'nan' else y))

        # Convert the 'postal' column to string
        if 'postalcode' in df.columns:
            df['postalcode'] = df['postalcode'].astype(str)

        # Convert date columns to datetime objects using df.apply
        df[date_columns] = df[date_columns].apply(pd.to_datetime, errors='coerce')
        return_dict = {
            'prepared_df': df,
            'success': True
        }
        return return_dict
    except Exception as e:
        print(f"Error preparing data for insert: {str(e)}")
        return_dict = {
            'prepared_df': None,
            'success': False
        }
        return return_dict

def insert_data_into_db(df, table_name):

    try:
        df.to_sql(table_name, dbp.create_database_engine(), if_exists='append', schema='asq', index=False)
        print (f"Data inserted into {table_name}")
        print (f"Updating respondent_id where respondent is duplicated in {table_name}")
        dbp.update_respondent_id_where_respondent_duplicated_in_file(df.asq_test_filename.unique()[0])
        return True
    except Exception as e:
        print(f"Error inserting data into {table_name}: {str(e)}")
        return False

def fetch_invalid_test_data(file_name):
    try:
        invalid_records_df = dbp.fetch_invalid_test_data(file_name)
        test_respondent_id_list = invalid_records_df['test_respondent_id'].tolist()
        invalid_records_df = invalid_records_df.drop(columns=['test_respondent_id'])
        dbp.invalidate_invalid_test_results(test_respondent_id_list)
        return invalid_records_df
    except Exception as e:
        print(f"Error setting invalid test results: {str(e)}")
        return invalid_records_df


def process_mapping_dataframe(df, rename_dict, file_name):
        return_dict = {
            'file_name': file_name,
            'invalid_records': None,
            'submitted_rows_count': 0,
            'errors': [],
        }
        submitted_rows_count = 0
        try:
            submitted_rows_count = df.shape[0]
            return_dict['submitted_rows_count'] = submitted_rows_count

            prepared_df_results = prepare_df_for_insert(df, rename_dict, file_name)
            prepared_df_results['prepared_df'] = aso.apply_asq_recommendation(prepared_df_results['prepared_df'])
            if prepared_df_results['success'] is True:
                df_inserted = insert_data_into_db(prepared_df_results['prepared_df'], 'asq_test_details')
            if df_inserted is True:
                invalid_records_df = fetch_invalid_test_data(file_name)
            if invalid_records_df is None or invalid_records_df.empty:
                return return_dict
            else:
                invalid_records_df[date_columns] = invalid_records_df[date_columns].apply(lambda col: col.map(lambda x: '' if x is None else str(x)))
                json_str = invalid_records_df.to_json(orient='records')
                json_obj = json.loads(json_str)
                return_dict['invalid_records'] = json_obj

            return return_dict
        except Exception as e:
            error_message = f'Error processing .xlsx spreadsheet: {str(e)}'
            print(error_message)
            return_dict['errors'].append(error_message)
            return return_dict



def process_mapping_submission(downloaded_blob_properties, rename_dict, file_name):
    content_type = downloaded_blob_properties['content_type']
    downloaded_blob = downloaded_blob_properties['downloaded_blob']

    df = return_dataframe_from_blob(downloaded_blob, content_type)

    return_data = process_mapping_dataframe(df, rename_dict, file_name)
    return return_data

def process_records_matching(file_name):
    return_data = {
        'partials_count': None,
        'errors': [],
    }
    matches_already_run = dbp.check_if_matches_exist(file_name)
    if matches_already_run > 0:
        partials_count = {}
        partials_count['Number of Partials'] = matches_already_run
        print(f"Matches already run for {file_name}")
    else:
        partials_count = match_processing.process_record_matching(file_name)
    if partials_count['Number of Partials'] > 0:
        partials_records = dbp.fetch_partial_matches(file_name)
        return_data['partials_count'] = partials_count['Number of Partials']
        return_data['partials_records'] = partials_records
    else:
        return return_data

    return return_data
