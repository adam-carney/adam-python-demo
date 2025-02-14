import pandas as pd
import database_processing as dbp


engine = dbp.create_database_engine()
ASQ_SCORE_MONITORING_RANGES = pd.read_sql_table("asq_score_monitoring_ranges", engine, schema='asq', index_col='score_range_id')


def determine_asq_recommendation(score_type,test_interval, test_score):
    score_range = ASQ_SCORE_MONITORING_RANGES.query("score_type == @score_type").query("test_interval == @test_interval")

    if test_score < score_range['monitoring_cutoff_low'].values[0]:
        return "Referral"
    elif score_range['monitoring_cutoff_low'].values[0] <= test_score <= score_range['monitoring_cutoff_high'].values[0]:
        return "Monitor"
    else:
        return None

def determine_asq_outcome(score_type,test_interval, test_score):
    score_range = ASQ_SCORE_MONITORING_RANGES.query("score_type == @score_type").query("test_interval == @test_interval")

    if test_score < score_range['monitoring_cutoff_low'].values[0]:
        return "Below Cutoff"
    elif score_range['monitoring_cutoff_low'].values[0] <= test_score <= score_range['monitoring_cutoff_high'].values[0]:
        return None
    else:
        return "Above Cutoff"

def apply_asq_recommendation(df):
    for score_type in ['communication', 'personal_social', 'fine_motor', 'gross_motor', 'problem_solving']:
        df[f"{score_type}_recommendation"] = df.apply(lambda x: determine_asq_recommendation(f"{score_type}_score", x['test_interval'], x[f"{score_type}_score"]), axis=1)
        df[f"{score_type}_outcome"] = df.apply(lambda x: determine_asq_outcome(f"{score_type}_score", x['test_interval'], x[f"{score_type}_score"]), axis=1)
    return df


if __name__ == "__main__":
    # Create a test dataframe
    test_data = {
        'test_interval': [2, 6, 48, 54, 60],
        'communication_score': [10, 20, 30, 40, 50],
        'personal_social_score': [15, 25, 35, 45, 55],
        'fine_motor_score': [12, 22, 32, 42, 52],
        'gross_motor_score': [14, 24, 34, 44, 54],
        'problem_solving_score': [16, 26, 36, 46, 56]
    }

    test_df = pd.DataFrame(test_data)

    # Apply ASQ recommendation
    result_df = apply_asq_recommendation(test_df)

    # Print the result
    print(result_df)