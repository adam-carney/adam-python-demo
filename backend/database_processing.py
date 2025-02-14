import sqlalchemy as sqla
from dotenv import load_dotenv
from sqlalchemy.sql import text, func
from sqlalchemy import create_engine, MetaData, select, Table, update
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.engine import Engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import OperationalError
import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

def create_database_engine():
    POSTGRES_HOST = os.getenv('POSTGRES_HOST')
    POSTGRES_USERNAME = os.getenv('POSTGRES_USER')
    POSTGRES_DATABASE = os.getenv('POSTGRES_DB')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

    conn_str = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DATABASE}"
    engine = sqla.create_engine(conn_str)
    return engine


def get_respondent_addresses() -> pd.DataFrame:
    engine = create_database_engine()
    metadata = MetaData(schema='directory')
    respondent_address = Table('respondent_address', metadata, autoload_with=engine, schema='directory')

    query = select(
        respondent_address.c.respondent_address_id,
        respondent_address.c.street_address,
        respondent_address.c.city,
        respondent_address.c.state,
        respondent_address.c.postalcode,
        respondent_address.c.census_tract_id,
        respondent_address.c.census_tract_lat,
        respondent_address.c.census_tract_lon
    )

    try:
        with engine.connect() as connection:
            result = connection.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            logger.info(f"Fetched {len(df)} respondent addresses")
            return df
    except OperationalError as e:
        logger.error(f"Operational error occurred: {e}")
        raise
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise

def get_respondent_address_by_filename(asq_filename: str) -> pd.DataFrame:
    engine = create_database_engine()
    metadata = MetaData(schema='asq')
    asq_test_details = Table('asq_test_details', metadata, autoload_with=engine, schema='asq')

    query = select(
        asq_test_details.c.respondent_address_id,
        asq_test_details.c.respondent_id,
        asq_test_details.c.street_address,
        asq_test_details.c.city,
        asq_test_details.c.state,
        asq_test_details.c.postalcode
    ).where(
        asq_test_details.c.asq_test_filename == asq_filename
    )

    try:
        with engine.connect() as connection:
            result = connection.execute(query)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            logger.info(f"Fetched {len(df)} addresses for file: {asq_filename}")
            return df
    except OperationalError as e:
        logger.error(f"Operational error occurred: {e}")
        raise
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise

def insert_job_status(job_state, asq_filename, user_id, organization_id):
    engine = create_database_engine()
    db = engine.connect()

    if job_state == 2:
        try:
            insert_query = text("""
                INSERT INTO asq.asq_job_status
                (
                    job_state,
                    asq_filename,
                    user_id,
                    organization_id
                )
                VALUES (
                    :job_state,
                    :asq_filename,
                    :user_id,
                    :organization_id
                )
            """)
            db.execute(insert_query, {
                'job_state': job_state,
                'asq_filename': asq_filename,
                'user_id': user_id,
                'organization_id': organization_id
            })
            db.commit()
            print(f"Job state inserted into db: {job_state}")
            return "success"
        except OperationalError as e:
            print(f"Operational error occurred: {e}")
            db.rollback()
            return "error"
        except Exception as e:
            print(f"Error occurred: {e}")
            db.rollback()
            return "error"
    else:
        try:
            update_query = text("""
                UPDATE asq.asq_job_status
                SET job_state = :job_state
                WHERE asq_filename = :asq_filename
            """)
            db.execute(update_query, {
                'job_state': job_state,
                'asq_filename': asq_filename
            })

            db.commit()

            print(f"Job state updated in db: {job_state}")
            return "success"
        except OperationalError as e:
            print(f"Operational error occurred: {e}")
            db.rollback()
            return "error"
        except Exception as e:
            print(f"Error occurred: {e}")
            db.rollback()
            return "error"

def fetch_schema_headers_from_db():
    engine = create_database_engine()
    header_sql = """
        select
            atdc.column_value
        from
            asq.asq_test_detail_cols atdc
        where
            atdc.is_active is true
        order by atdc.friendly_name asc
        """
    header_df = pd.read_sql(header_sql, engine)
    return header_df.column_value.to_list()

def fetch_init_file_headers():
    engine = create_database_engine()
    init_headers_sql = """
    select
        atdc.column_value,
        atdc.friendly_name,
        atdc.is_required
    from
        asq.asq_test_detail_cols atdc
    where
        atdc.is_active is true
    and
        atdc.column_value not in ('asq_test_filename')
    order by atdc.friendly_name asc
    """
    header_df = pd.read_sql(init_headers_sql, engine)
    return header_df.to_dict(orient='records')

def get_asq_test_details_table() -> Table:
    metadata = MetaData(schema='asq')
    core_engine = create_database_engine()
    return Table('asq_test_details', metadata, autoload_with=core_engine, schema='asq')

def update_new_respondents(test_file_name):
    update_statement = f"""UPDATE asq.asq_test_details SET respondent_match_status = 'NEW'
        WHERE  asq.asq_test_details.asq_record_valid = true
        AND asq.asq_test_details.respondent_id
        IN (
            SELECT respondent_id FROM asq.asq_test_details atd WHERE atd.asq_record_valid = true
            AND atd.respondent_match_status IS NULL
            AND atd.asq_test_filename = '{test_file_name}'
            AND atd.respondent_id NOT IN (SELECT unique_id_r
            FROM asq.asq_directory_matching_summary adms
            WHERE asq_test_filename = '{test_file_name}'
            )
        )"""
    db_engine = create_database_engine()
    with db_engine.connect() as conn:
        conn.execute(text(update_statement))
        conn.commit()

def update_respondent_id_where_respondent_duplicated_in_file(file_name):
    update_respondent_id_sql = text(f"""update asq.asq_test_details
            set respondent_id = duplicate.respondent_id,
            respondent_address_id = duplicate.respondent_address_id
            from
        (select first_value(respondent_id) over (partition by resp_string, asq_test_filename) as respondent_id,
            first_value(respondent_address_id) over (partition by resp_string, asq_test_filename) as respondent_address_id,
            test_respondent_id,
            rnk
            from
            (select concat(first_name, middle_name, last_name, gender, race_ethnicity, street_address, city, state, postalcode) as resp_string,
            rank() over (partition by concat(first_name, middle_name, last_name, gender, race_ethnicity, street_address, city, state, postalcode) order by test_respondent_id) rnk,
            test_respondent_id,
            respondent_id,
            respondent_address_id,
            asq_test_filename
            from asq.asq_test_details atd where asq_test_filename = '{file_name}'
            order by rnk) as duplicate
            where rnk > 1)""")
    try:
        engine = create_database_engine()
        with engine.connect() as connection:
            connection.execute(update_respondent_id_sql)
            connection.commit()
            connection.close()
    except Exception as e:
        print(f"Error updating respondent_id where respondent duplicated in file: {str(e)}")


def get_asq_directory_matching_summary_table() -> Table:
    metadata = MetaData(schema='asq')
    core_engine = create_database_engine()
    return Table('asq_directory_matching_summary', metadata, autoload_with=core_engine, schema='asq')


def update_exact_match_status(filename):
    exact_match_update_sql = text(f"""UPDATE asq.asq_test_details atd
        SET respondent_match_status = 'MATCHED',
        respondent_id = r.respondent_id,
        respondent_address_id = ra.respondent_address_id
        FROM directory.respondent r
        JOIN directory.respondent_address_map ram ON r.respondent_id = ram.respondent_id
        JOIN directory.respondent_address ra ON ram.respondent_address_id = ra.respondent_address_id
        WHERE atd.first_name = r.first_name
        AND atd.middle_name = r.middle_name
        AND atd.last_name = r.last_name
        AND atd.gender = r.gender
        AND atd.birthdate = r.birthdate
        AND atd.street_address = ra.street_address
        AND atd.city = ra.city
        AND atd.state = ra.state
        AND atd.postalcode = ra.postalcode
        AND atd.asq_test_filename = '{filename}'
        AND atd.asq_record_valid = true;
        """)
    try:
        engine = create_database_engine()
        with engine.connect() as connection:
            connection.execute(exact_match_update_sql)
            connection.commit()
            connection.close()
    except Exception as e:
        print(f"Error updating exact match status: {str(e)}")

def fetch_partial_matches(file_name):
    asq_directory_matching_summary = get_asq_directory_matching_summary_table()
    # file_name = 'asq_asq3_v2_100_20240902152356_190989ae.xlsx'
    query = select(
        asq_directory_matching_summary.c.unique_id_l.label('directory_respondent_id'),
        asq_directory_matching_summary.c.first_name_l.label('directory_first_name'),
        asq_directory_matching_summary.c.middle_name_l.label('directory_middle_name'),
        asq_directory_matching_summary.c.last_name_l.label('directory_last_name'),
        asq_directory_matching_summary.c.birthdate_l.label('directory_birthdate'),
        asq_directory_matching_summary.c.gender_l.label('directory_gender'),
        asq_directory_matching_summary.c.street_address_l.label('directory_address'),
        asq_directory_matching_summary.c.state_l.label('directory_state'),
        asq_directory_matching_summary.c.postalcode_l.label('directory_zip'),
        asq_directory_matching_summary.c.unique_id_r.label('asq_record_respondent_id'),
        asq_directory_matching_summary.c.first_name_r.label('asq_record_first_name'),
        asq_directory_matching_summary.c.middle_name_r.label('asq_record_middle_name'),
        asq_directory_matching_summary.c.last_name_r.label('asq_record_last_name'),
        asq_directory_matching_summary.c.birthdate_r.label('asq_record_birthdate'),
        asq_directory_matching_summary.c.gender_r.label('asq_record_gender'),
        asq_directory_matching_summary.c.street_address_r.label('asq_record_address'),
        asq_directory_matching_summary.c.state_r.label('asq_record_state'),
        asq_directory_matching_summary.c.postalcode_r.label('asq_record_zip')
    ).where(
        asq_directory_matching_summary.c.asq_test_filename == file_name
    ).distinct()
    data_list_of_dicts = []
    session = create_database_engine().connect()
    try:
        result = session.execute(query)
        rows = result.fetchall()
        keys = result.keys()
        data_list_of_dicts = [dict(zip(keys, row)) for row in rows]
        logger.info(f"Fetched {len(rows)} partial matches for file: {file_name}")
    except OperationalError as e:
        logger.error(f"Operational error occurred: {e}")
        raise
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise

    return data_list_of_dicts


def check_if_matches_exist(file_name):
    data = fetch_partial_matches(file_name)
    return len(data)

def update_reconciled_status(reconcile_list, fileName):
    engine = create_database_engine()
    db = engine.connect()
    try:
        for selection in reconcile_list:
            reconcile_type = selection['reconcile_type']
            asq_record_respondent_id = selection['asq_record_respondent_id']
            pre_check_sql = text(f"""
                select
                    case when respondent_match_status is not null
                    then true
                    else false
                    end as already_updated
                    from
                    asq.asq_test_details atd
                    where
                    asq_test_filename = '{fileName}'
                    and respondent_id = '{asq_record_respondent_id}'
                """)
            is_updated = db.execute(pre_check_sql)

            if is_updated == True:
                print(f"Record already updated")
                continue
            else:
                update_query = text(f"""
                UPDATE asq.asq_test_details
                SET respondent_match_status = '{reconcile_type}'
                WHERE respondent_id = '{asq_record_respondent_id}'
            """)
                db.execute(update_query)
                db.commit()
        print(f"Reconciled status updated in db")
        return "success"
    except OperationalError as e:
        print(f"Operational error occurred: {e}")
        db.rollback()
        return "error"
    except Exception as e:
        print(f"Error occurred: {e}")
        db.rollback()
        return "error"

def fetch_invalid_test_data(filename):
    # filename = 'asq_asq3_v2_100_20240914152324_4eb824a9.xlsx'
    print(f'file name: {filename}')
    query = f"""
        select
            atd.test_respondent_id,
            atd.first_name,
            atd.middle_name,
            atd.last_name,
            atd.birthdate,
            atd.gender,
            atd.race_ethnicity,
            atd.street_address,
            atd.city,
            atd.state,
            atd.postalcode,
            atd.premature,
            atd.test_date,
            atd.test_type,
            atd.test_interval,
            atd.program,
            atd.agency,
            atd.source,
            atd.origin,
            atd.test_location,
            atd.proxy_first_name,
            atd.proxy_last_name,
            atd.proxy_relationship,
            atd.communication_score,
            atd.communication_outcome,
            atd.communication_recommendation,
            atd.gross_motor_score,
            atd.gross_motor_outcome,
            atd.gross_motor_recommendation,
            atd.fine_motor_score,
            atd.fine_motor_outcome,
            atd.fine_motor_recommendation,
            atd.problem_solving_score,
            atd.problem_solving_outcome,
            atd.problem_solving_recommendation,
            atd.personal_social_score,
            atd.personal_social_outcome,
            atd.personal_social_recommendation,
            atd.overall_test_score,
            atd.referral,
            atd.referral_concern,
            atd.referral_agency,
            atd.referral_followup_date,
            atd.referral_source,
            atd.referral_result,
            atd.test_language,
            atvv.last_name_valid,
            atvv.birthdate_valid,
            atvv.is_job_unique,
            atvv.is_respondent_unique
        from
            asq.asq_test_details atd
        join
            asq.asq_tests_valid_v atvv on
            atvv.test_respondent_id = atd.test_respondent_id
        and
            (atvv.last_name_valid is false
            or atvv.birthdate_valid is false
            or atvv.is_job_unique is false
            or atvv.is_respondent_unique is false)
        where
            atd.asq_test_filename = '{filename}'
    """

    try:
        engine = create_database_engine()
        invalid_records_df = pd.read_sql(query, engine)
        return invalid_records_df
    except Exception as e:
        print(f"Error selecting invalid test results: {str(e)}")
        return {'file_contents': None, 'errors': [f"Error selecting invalid test results: {str(e)}"]}

def invalidate_invalid_test_results(test_respondent_id_list):
    if not test_respondent_id_list:
        return None

    core_engine = create_database_engine()
    try:
        with core_engine.connect() as connection:
            asq_test_details = get_asq_test_details_table()
            stmt = update(asq_test_details).where(asq_test_details.c.test_respondent_id.in_(test_respondent_id_list)).values(asq_record_valid=False)
            connection.execute(stmt)
            connection.commit()
            connection.close()
    except Exception as e:
        print(f"Error executing update statement: {str(e)}")
        return None
    return None

def fetch_my_jobs(user_id_list: list):
    print(f'user_id: {user_id_list}')

    formatted_user_id_list = ', '.join(f"'{user_id}'" for user_id in user_id_list)
    print(f'formatted_user_id_list: {formatted_user_id_list}')
    # 'auth0|675b58eb5cc1d7bd4f246db1', 'auth0|676869d66a0a0ae37ab1574d', 'auth0|67044e3284ce49af0a638f10'
    query = f"""
        SELECT
            ajs.asq_job_status_id,
            ajs.asq_filename,
            ajs.user_id,
            ajs.organization_id,
            ajs.job_state,
            ajst.job_state_description,
            ajs.insert_timestamp,
            ajs.update_timestamp
        FROM
            asq.asq_job_status ajs
        JOIN
            asq.asq_job_state_types ajst on
            ajst.job_state = ajs.job_state
        WHERE
            ajs.user_id in ({formatted_user_id_list})
        AND
            ajs.job_state > 0
        ORDER BY
            ajs.insert_timestamp desc
    """

    try:
        engine = create_database_engine()
        my_jobs_df = pd.read_sql(query, engine)
        return my_jobs_df
    except Exception as e:
        print(f"Error fetching user jobs: {str(e)}")
        return pd.DataFrame()

def fetch_directory():
    print('fetching directory')
    query = f"""
        SELECT
            r.respondent_id,
            r.first_name,
            r.middle_name,
            r.last_name,
            r.birthdate,
            r.gender,
            r.race_ethnicity,
            r.insert_datetime,
            r.update_timestamp
        FROM
            directory.respondent r
        ORDER BY
            r.last_name,
            r.first_name,
            r.middle_name
    """

    try:
        engine = create_database_engine()
        directory_df = pd.read_sql(query, engine)
        return directory_df
    except Exception as e:
        print(f"Error fetching user jobs: {str(e)}")
        return pd.DataFrame()


def archive_job(job_id: int):
    print(f'job_id:  {type(job_id)}, {job_id}')
    query = """
    UPDATE asq.asq_job_status
    SET job_state = -1
    WHERE asq_job_status_id = :job_id
    """

    engine = create_database_engine()
    db = engine.connect()

    try:
        result = db.execute(text(query), {"job_id": job_id})
        db.commit()
        if result.rowcount > 0:
            print(f"Job state updated in db: {job_id}")
            return {"status": "success", "message": f"Job {job_id} archived successfully."}
        else:
            print(f"Job {job_id} not found.")
            return {"status": "error", "message": f"Job {job_id} not found."}
    except OperationalError as e:
        print(f"Operational error occurred: {e}")
        db.rollback()
        return {"status": "error", "message": f"Operational error occurred: {e}"}
    except Exception as e:
        print(f"Error occurred: {e}")
        db.rollback()
        return {"status": "error", "message": f"Error occurred: {e}"}
    finally:
        db.close()


def fetch_respondent_matches(file_name: str):
    print(f'file_name: {file_name}')
    query = f"""
        SELECT
            atd.first_name,
            atd.middle_name,
            atd.last_name,
            atd.respondent_match_status
        FROM
            asq.asq_test_details atd
        WHERE
            atd.asq_test_filename = '{file_name}'
        ORDER BY
            atd.first_name,
            atd.middle_name,
            atd.last_name
    """

    try:
        engine = create_database_engine()
        matched_respondents_df = pd.read_sql(query, engine)
        return matched_respondents_df
    except Exception as e:
        print(f"Error fetching user jobs: {str(e)}")
        return pd.DataFrame()


if __name__ == '__main__':
    print (update_exact_match_status('mock_respondent_data_upload_20250209101958_6a37ab40.xlsx'))