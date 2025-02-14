import database_processing as dbp
from splink import DuckDBAPI
from splink.exploratory import profile_columns
import os
import pandas as pd
import re
import string
import json
from sqlalchemy import text
from splink import DuckDBAPI, block_on
from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)
import splink.comparison_library as cl
import splink.comparison_level_library as cll
from splink import Linker, SettingsCreator
from sqlalchemy import create_engine
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# from dotenv import load_dotenv

MATCH_SUMMARY_INSERT_LIST = ['match_weight', 'match_probability', 'source_dataset_l',
                             'source_dataset_r', 'unique_id_l', 'unique_id_r', 'first_name_l',
                             'first_name_r', 'last_name_l', 'last_name_r',
                             'first_name_last_name_concat_l', 'first_name_last_name_concat_r',
                             'middle_name_l', 'middle_name_r', 'postalcode_l',
                             'postalcode_r', 'city_l', 'city_r', 'street_address_l', 'street_address_r', 'state_l', 'state_r',
                             'birthdate_l', 'birthdate_r', 'gender_l', 'gender_r', 'asq_test_filename', 'model_type']

engine = dbp.create_database_engine()

__SUFFIX_LIST = ['I', 'II', 'III', 'IV', 'V', 'SR', 'JR',
                 'L', 'LL', 'LLL', 'IL', 'MR', 'MRS', 'MS',
                 'DR', 'DDS', 'DVM', 'PHD', 'MD', 'DO', 'DC',
                 'OD', 'PA', 'RN', 'NP', 'APRN', 'CRNA', 'CNS',
                 'CNM', 'CNA', 'LPN', 'LVN', 'PT', 'OT', 'SLP']


def name_cleanup(
        name: str) -> str:
    """
    Cleans up the name by removing any suffix data and returning cleaned up name
    @param name:
    @return:
    """
    if len(name) > 1:
        name = re.compile('[%s]' % re.escape(
            string.punctuation)).sub(' ', name)
        name_cleanup = ' '.join([name_part for name_part in name.strip().upper().split(' ')
                                 if not name_part in __SUFFIX_LIST and not name_part.strip() == ''])
    else:
        name_cleanup = name

    return name_cleanup.title()


def process_matching(test_df, directory_df, filename, model_type):
    first_name_min = test_df['first_name'].str.len().min()
    last_name_min = test_df['last_name'].str.len().min()
    if first_name_min == 0:
        first_name_min = 1
    if last_name_min == 0:
        last_name_min = 1

    db_api = DuckDBAPI()
    if first_name_min + last_name_min <= 2:
        model_name = 'initials.json'
        match_prob = 0.9
    else:
        model_name = 'regular.json'
        match_prob = 0.99999
    linker = Linker([directory_df, test_df], settings=model_name, db_api=db_api,
                    input_table_aliases=["directory", "loadfile"], set_up_basic_logging=False)

    try:
        df_predict = linker.inference.predict(
            threshold_match_probability=match_prob)
        df_e = df_predict.as_pandas_dataframe()
        df_e['asq_test_filename'] = filename
        df_e['model_type'] = model_type
        asq_directory_match_json = json.dumps(df_predict.as_record_dict())
        asq_directory_match_model = json.dumps(
            linker.misc.save_model_to_json())
        log_df = pd.DataFrame(columns=['asq_test_filename', 'asq_directory_match_json', 'asq_directory_match_model'], data=[
                              [filename, asq_directory_match_json, asq_directory_match_model]])
        log_df.to_sql('asq_directory_match_log', engine,
                      if_exists='append', schema='asq', index=False)
        df_e[MATCH_SUMMARY_INSERT_LIST].to_sql(
            'asq_directory_matching_summary', engine, if_exists='append', schema='asq', index=False)
        return df_e
    except Exception as e:
        print(f'Predictor failed')
        df_e = pd.DataFrame()
        return df_e


def setup_matching(loadfile_df, directory_df, model_file_name, model_type):
    # loadfile_name = loadfile_directory + '/' + filename + '.xlsx'
    directory_df['first_name'] = directory_df['first_name'].apply(name_cleanup)
    directory_df['last_name'] = directory_df['last_name'].apply(name_cleanup)
    loadfile_df['first_name'] = loadfile_df['first_name'].apply(name_cleanup)
    directory_df["first_name_last_name_concat"] = directory_df["first_name"] + \
        " " + directory_df["last_name"]
    loadfile_df["first_name_last_name_concat"] = loadfile_df["first_name"] + \
        " " + loadfile_df["last_name"]
    df = process_matching(loadfile_df, directory_df,
                          model_file_name, model_type)
    return df


def process_record_matching(test_file_name):
    dbp.update_exact_match_status(test_file_name)
    direct_df = pd.read_sql("""SELECT r.respondent_id as unique_id, r.first_name, r.middle_name, r.last_name, r.birthdate, r.gender, ra.street_address, ra.city, ra.state, ra.postalcode 
    FROM directory.respondent r
    join directory.respondent_address_map ram on
                r.respondent_id = ram.respondent_id 
    join directory.respondent_address ra on ram.respondent_address_id = ra.respondent_address_id """, engine).astype(str)
    testing_df = pd.read_sql(f"""SELECT respondent_id as unique_id, first_name, middle_name, last_name, birthdate, gender, street_address, city, state, postalcode FROM asq.asq_test_details where asq_test_filename = '{
                                test_file_name}' and asq_record_valid = True and respondent_match_status is null""", engine).astype(str)
    initials_df = testing_df[(testing_df['first_name'].str.len() == 1) | (
        testing_df['last_name'].str.len() == 1)]
    regular_name_df = testing_df[~testing_df.index.isin(initials_df.index)]

    if len(initials_df) > 0:
        init_processing_df = setup_matching(
            initials_df, direct_df, test_file_name, model_type='initials')
        # output_filename = loadfile_directory + '/' + output_name + 'init_names_results.xlsx'
        # init_processing_df.to_excel(output_filename, index=False)

    if len(regular_name_df) > 0:
        reg_name_df = setup_matching(
            regular_name_df, direct_df, test_file_name, model_type='regular')
        # output_filename = loadfile_directory + '/' + output_name + 'reg_names_results.xlsx'
        # reg_name_df.to_excel(output_filename, index=False)

    if not initials_df.empty and not regular_name_df.empty:
        concatenated_df = pd.concat(
            [init_processing_df, reg_name_df], ignore_index=True)
        return_dict = {'Number of Partials': len(concatenated_df)}
    elif not regular_name_df.empty:
        return_dict = {'Number of Partials': len(reg_name_df)}
    elif not initials_df.empty:
        return_dict = {'Number of Partials': len(init_processing_df)}
    else:
        return_dict = {'Number of Partials': 0}

    print(f'return dict at the end: {return_dict}')
    return return_dict


if __name__ == '__main__':
    print(process_record_matching(
        "mock_respondent_data-clean_20241225095303_84ddec7f.xlsx"))
