import os
import duckdb
import pandas as pd
import re
import string
import json
import traceback
from sqlalchemy.engine import Engine
from sqlalchemy import text
from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)
import splink.comparison_library as cl
import splink.comparison_level_library as cll
import os
import pandas as pd
import re
import string
import json
import traceback
from sqlalchemy import text
from splink import DuckDBAPI, block_on, Linker, SettingsCreator
from splink.blocking_analysis import (
    cumulative_comparisons_to_be_scored_from_blocking_rules_chart,
)
import splink.comparison_library as cl
import splink.comparison_level_library as cll

import database_processing as dbp
import logging
engine = dbp.create_database_engine()
# Configure Splink's logger
splink_logger = logging.getLogger("splink")
splink_logger.setLevel(logging.ERROR)  # Set the desired log level 

# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Change the current working directory to the script directory
os.chdir(script_dir)

print(f"Current working directory: {os.getcwd()}")


MATCH_SUMMARY_INSERT_LIST = ['match_weight', 'match_probability', 'source_dataset_l',
           'source_dataset_r', 'unique_id_l', 'unique_id_r', 'first_name_l',
           'first_name_r', 'last_name_l', 'last_name_r',
           'first_name_last_name_concat_l', 'first_name_last_name_concat_r',
           'middle_name_l', 'middle_name_r','postalcode_l',
           'postalcode_r','city_l', 'city_r','street_address_l', 'street_address_r','state_l', 'state_r',
           'birthdate_l', 'birthdate_r', 'gender_l', 'gender_r', 'asq_test_filename', 'model_type']

__SUFFIX_LIST = ['I', 'II', 'III', 'IV', 'V', 'SR', 'JR',
                     'L', 'LL', 'LLL', 'IL', 'MR', 'MRS', 'MS',
                     'DR', 'DDS', 'DVM', 'PHD', 'MD', 'DO', 'DC',
                     'OD', 'PA', 'RN', 'NP', 'APRN', 'CRNA', 'CNS',
                     'CNM', 'CNA', 'LPN', 'LVN', 'PT', 'OT', 'SLP']



def name_cleanup(name: str) -> str:
    if len(name) > 1:
        name = re.compile('[%s]' % re.escape(string.punctuation)).sub(' ', name)
        name_cleanup = ' '.join([name_part for name_part in name.strip().upper().split(' ') \
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

    blocking_rules = [
        block_on("first_name","last_name"),#, "birthdate", "gender"),
        block_on("substr(first_name,1," + str(first_name_min) + ")",  "substr(last_name,1," + str(last_name_min) + ")" , "gender", "city"),
        block_on("birthdate"),
    ]

    db_api = DuckDBAPI()
    if first_name_min <= 1 or last_name_min <= 1:
        settings = SettingsCreator(
            link_type="link_only",
            blocking_rules_to_generate_predictions=blocking_rules,
            comparisons=[
                cl.ForenameSurnameComparison(
                    "first_name",
                    "last_name",
                    forename_surname_concat_col_name="first_name_last_name_concat",
                ),
                cl.DateOfBirthComparison(
                    "birthdate", input_is_string=True
                ),
                cl.NameComparison("gender"),
                cl.ExactMatch("postalcode"),
                cl.NameComparison("street_address"),
                cl.NameComparison("city"),
            ],
            retain_intermediate_calculation_columns=True,
        )
    else:
        settings = SettingsCreator(
            link_type="link_only",
            blocking_rules_to_generate_predictions=blocking_rules,
            comparisons=[
                cl.ForenameSurnameComparison(
                    "first_name",
                    "last_name",
                    forename_surname_concat_col_name="first_name_last_name_concat",
                ),
                cl.DateOfBirthComparison(
                    "birthdate", input_is_string=True
                ),
                cl.NameComparison("gender"),
                cl.NameComparison("first_name"),
                cl.NameComparison("last_name"),
                cl.NameComparison("middle_name"),
                cl.ExactMatch("postalcode"),
                cl.NameComparison("street_address"),
                cl.NameComparison("city"),
            ],
            retain_intermediate_calculation_columns=True,
        )
        
    linker = Linker([directory_df, test_df], settings, db_api=db_api, input_table_aliases=["directory", "loadfile"])
    if first_name_min <= 1 or last_name_min <= 1:
        recall_value = 0.5
        estimate_prob_string = "(substr(l.first_name, 1," + str(first_name_min) + ") = substr(r.first_name, 1, "+ str(first_name_min) + ") and substr(l.last_name, 1, " + str(last_name_min) + ") = substr(r.last_name, 1, " + str(last_name_min) + "))"
        session_names_fname_rule = "substr(first_name,1," + str(first_name_min) + ")"
        session_names_lname_rule = "substr(last_name,1," + str(last_name_min) + ")"
        match_probability = 0.999999
        max_pairs = 1e4
        model_name = 'initials.json'
    else:
        recall_value = 0.8
        estimate_prob_string = "substr(l.first_name, 1, 3) = substr(r.first_name, 1, 3) and substr(l.last_name, 1, 3) = substr(r.last_name, 1,3)"
        session_names_fname_rule =  "substr(first_name,1," + str(first_name_min) + ")"
        session_names_lname_rule = "substr(last_name,1," + str(last_name_min) + ")"
        match_probability = 0.9
        max_pairs = 1e5
        model_name = 'regular.json'

    linker.training.estimate_u_using_random_sampling(max_pairs=max_pairs, seed=42)
    training_blocking_rule = block_on(session_names_fname_rule, session_names_lname_rule)

    try:
        training_session_names = (
            linker.training.estimate_parameters_using_expectation_maximisation(
            training_blocking_rule, estimate_without_term_frequencies=True
            )
        )
    except:
        training_session_names = (
            linker.training.estimate_probability_two_random_records_match(
            "(l.first_name = r.last_name or l.last_name = r.first_name) and l.birthdate = r.birthdate", recall=recall_value
            )
        )
    training_blocking_rule = block_on("birthdate", "gender", "street_address")
    try:
        training_session_dob = (
        linker.training.estimate_parameters_using_expectation_maximisation(
            training_blocking_rule, estimate_without_term_frequencies=False
            )
        )
    except Exception as e:
        print("Error in training session dob: " + repr(e))
        pass

    try:
        df_predict = linker.inference.predict(threshold_match_probability=match_probability)
        df_e = df_predict.as_pandas_dataframe()
        df_e['asq_test_filename'] = filename
        df_e['model_type'] = model_type
        directory_df_subset = directory_df[['unique_id', 'state']].rename(columns={'state': 'state_l', 'unique_id': 'unique_id_l'})
        df_e = df_e.merge(directory_df_subset, on='unique_id_l', how='left')
        test_df_subset = test_df[['unique_id', 'state']].rename(columns={'state': 'state_r', 'unique_id': 'unique_id_r'})
        df_e = df_e.merge(test_df_subset, on='unique_id_r', how='left')
        #asq_directory_match_json = json.dumps(df_predict.as_record_dict())
        linker.misc.save_model_to_json(model_name, overwrite=True)
        #log_df = pd.DataFrame(columns=['asq_test_filename', 'asq_directory_match_json', 'asq_directory_match_model'], data=[[filename, asq_directory_match_json, asq_directory_match_model]])
        #log_df_records = log_df.to_dict(orient='records')
        #print (f'log_df_records: {log_df_records}')
        #df_e[MATCH_SUMMARY_INSERT_LIST].to_sql('asq_directory_matching_summary', engine(), if_exists='append', schema='asq', index=False)
        #print (f'Predictor success: {df_e}')
        #new_resp_update_statement = f"UPDATE asq.asq_test_details SET respondent_match_status = 'NEW', INSERT_TIMESTAMP = NOW() WHERE respondent_id IN (SELECT respondent_id FROM asq.asq_test_details atd WHERE atd.asq_record_valid = true and atd.asq_test_filename = '{filename}' AND atd.respondent_id NOT IN (SELECT unique_id_r FROM asq.asq_directory_matching_summary adms WHERE asq_test_filename = '{filename}'))"
        #update_asq_test_details(new_resp_update_statement)
        #print (f'New respondent update statement: {new_resp_update_statement}')
        return df_e
    except Exception:
        print(f'Predictor failed:')
        traceback.print_exc()
        df_e = pd.DataFrame()
        return df_e

def setup_matching(loadfile_df, directory_df, model_file_name, model_type):
    directory_df['first_name'] = directory_df['first_name'].apply(name_cleanup)
    directory_df['last_name'] = directory_df['last_name'].apply(name_cleanup)
    loadfile_df = loadfile_df.copy()
    loadfile_df['first_name'] = loadfile_df['first_name'].apply(name_cleanup)
    directory_df["first_name_last_name_concat"] = directory_df["first_name"] + " " + directory_df["last_name"]
    loadfile_df["first_name_last_name_concat"] = loadfile_df["first_name"] + " " + loadfile_df["last_name"]
   
    df = process_matching(loadfile_df, directory_df, model_file_name, model_type)
    
    return df

def process_file_matching(file_name):
    try:
        direct_df = pd.read_sql("""SELECT respondent_id as unique_id, first_name, middle_name, last_name, birthdate, 
                                gender, street_address, city, state, postalcode FROM directory.respondent""", engine).astype(str)
        testing_df = pd.read_sql(f"""SELECT respondent_id as unique_id, first_name, middle_name, last_name, 
                                birthdate, gender, street_address, city, state, postalcode 
                                FROM asq.asq_test_details where asq_test_filename = '{file_name}' 
                                and asq_record_valid = True and test_respondent_id not in (
                                select 
                                atd.test_respondent_id 
                                from
                                asq.asq_test_details atd 
                                join directory.respondent r on atd.first_name = r.first_name 
                                and atd.middle_name = r.middle_name 
                                and atd.last_name = r.last_name 
                                and atd.gender = r.gender 
                                and atd.birthdate = r.birthdate 
                                and atd.street_address = r.street_address 
                                and atd.city = r.city 
                                and atd.state = r.street_address 
                                and atd.postalcode = r.postalcode 
                                where atd.asq_test_filename = '{file_name}' and atd.asq_record_valid = true)""", engine).astype(str)
        
        update_full_match = f"""update asq.asq_test_details 
            set respondent_match_status = 'MATCHED'
            where asq_test_details.test_respondent_id in (
            select 
            atd.test_respondent_id 
            from
            asq.asq_test_details atd 
            join directory.respondent r on atd.first_name = r.first_name 
            and atd.middle_name = r.middle_name 
            and atd.last_name = r.last_name 
            and atd.gender = r.gender 
            and atd.birthdate = r.birthdate 
            and atd.street_address = r.street_address 
            and atd.city = r.city 
            and atd.state = r.street_address 
            and atd.postalcode = r.postalcode 
            where atd.asq_test_filename = '{file_name}' and atd.asq_record_valid = true
            )"""
        splink_logger.info(f'Full match update statement: {update_full_match}')
        #update_asq_test_details(update_full_match)

        initials_df = testing_df[(testing_df['first_name'].str.len() == 1) | (testing_df['last_name'].str.len() == 1)]
        regular_name_df = testing_df[~testing_df.index.isin(initials_df.index)]

        if len(initials_df) > 0:
            init_processing_df = setup_matching(initials_df, direct_df, file_name, model_type='initials')

        if len(regular_name_df) > 0:
            reg_name_df = setup_matching(regular_name_df, direct_df, file_name, model_type='regular')

        if not initials_df.empty and not regular_name_df.empty:
            concatenated_df = pd.concat([init_processing_df, reg_name_df], ignore_index=True)
            return_dict = {'Number of Partials': len(concatenated_df)}
        elif not regular_name_df.empty:
            return_dict = {'Number of Partials': len(reg_name_df)}
        elif not initials_df.empty:
            return_dict = {'Number of Partials': len(init_processing_df)}
        else:
            return_dict = {'Number of Partials': 0}

        print(f'return dict at the end: {return_dict}')
        return return_dict
    except Exception as e:
        splink_logger.error(f'Error in main: {repr(e)}')
        traceback.print_exc()
        return {'Number of Partials': 0}


if __name__ == '__main__':
    print(process_file_matching("model_training_large.xlsx"))
