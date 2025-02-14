import pandas as pd
from splink import DuckDBAPI, block_on, Linker, SettingsCreator
import splink.comparison_library as cl
import splink.comparison_level_library as cll
import database_processing as dbp


def process_matching(test_df, directory_df):

    blocking_rules = [
        block_on("city","postalcode"),
    ]

    db_api = DuckDBAPI()

    settings = SettingsCreator(
        link_type="link_only",
        blocking_rules_to_generate_predictions=blocking_rules,
        comparisons=[
            cl.ExactMatch("postalcode"),
            cl.NameComparison("street_address"),
            cl.NameComparison("city"),
            cl.NameComparison("state"),
        ],
        retain_intermediate_calculation_columns=True,
    )
 
        
    linker = Linker([directory_df, test_df], settings, db_api=db_api, input_table_aliases=["directory", "loadfile"])

    match_probability = 0.9
    max_pairs = 1e5
    model_name = 'addr_matching.json'
    session_rule =  "(state = r.state and postalcode = r.postalcode)"
    linker.training.estimate_u_using_random_sampling(max_pairs=max_pairs, seed=42)
    training_blocking_rule = block_on(session_rule)

    try:
        training_session_names = (
            linker.training.estimate_parameters_using_expectation_maximisation(
            training_blocking_rule, estimate_without_term_frequencies=True
            )
        )
    except:
        training_session_names = (
            linker.training.estimate_probability_two_random_records_match(
            session_rule, recall=.7
            )
        )
    training_blocking_rule = block_on("street_address")
    try:
        training_session_dob = (
        linker.training.estimate_parameters_using_expectation_maximisation(
            training_blocking_rule, estimate_without_term_frequencies=False
            )
        )
    except Exception as e:
        print("Error in training session street: " + repr(e))
        pass

    try:
        df_predict = linker.inference.predict(threshold_match_probability=match_probability)
        df_e = df_predict.as_pandas_dataframe()
        return df_e
    except Exception:
        print(f'Predictor failed:')
        
        df_e = pd.DataFrame()
        return df_e




if __name__ == "__main__":
    
    asq_test_details = pd.read_sql("SELECT respondent_address_id as unique_id, street_address, city, state, postalcode FROM asq.asq_test_details where state = 'CA'", dbp.create_database_engine())
    respondent_address = pd.read_sql("SELECT respondent_address_id as unique_id, street_address, city, state, postalcode FROM directory.respondent_address", dbp.create_database_engine())
    df = process_matching(asq_test_details, respondent_address)
    
