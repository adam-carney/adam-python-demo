import database_processing as dbp
import pandas as pd
import models

#FILTERS FOR DATA VISUALIZATION
def agency_report_filter(organization_id: list):
    organization_ids = tuple(organization_id)
    try:
        columns = ', '.join(models.AgencyReportFilter.model_fields)
        agency_report_df = pd.read_sql(f"select distinct {columns} from directory.asq_test_data_v where organization_id IN ({','.join(['%s'] * len(organization_ids))})", dbp.create_database_engine(), params=organization_ids)
        return agency_report_df.to_dict(orient='records')
    except Exception as e:
        return str(e)


#RESPONDENT LEVEL DATA RETURNS
def return_respondent_cutoff_report(return_anon: bool, organization_id: list):
    organization_ids = tuple(organization_id)
    try:
        resp_cutoff_df = pd.read_sql(f"select * from directory.respondent_cutoff_report_v where organization_id IN ({','.join(['%s'] * len(organization_ids))})", dbp.create_database_engine(), params=organization_ids)
        if return_anon:
            return_df = resp_cutoff_df.drop(columns=['respondent_name'])
            return return_df.to_dict(orient='records')
        else:
            return_df = resp_cutoff_df.drop(columns=['respondent_id']).rename(columns={'respondent_name': 'respondent_id'})
            return return_df.to_dict(orient='records')
    except Exception as e:
        return str(e)






#AGENCY LEVEL DATA RETURNS
def test_interval_risk_factors(organization_id: list):
    organization_ids = tuple(organization_id)
    #THIS IS EASIER TO IMPLEMENT AS A SQL BECAUSE OF SUMMARIES IN MULTIPLE ORGS
    risk_sql = f"""
    select 
    test_interval_str,
    sum(communication_flag) as communication_risk,
    sum(gross_motor_flag) as gross_motor_risk,
    sum(fine_motor_flag) as fine_motor_risk,
    sum(personal_social_flag) as personal_social_risk,
    sum(problem_solving_flag) as problem_solving_risk,
    sum(test_domain_count) as test_domain_count
    from ( 
    select
    organization_id,
    test_interval_str,
    test_interval,
    case 
        when communication_recommendation is not null
        then 1
        else 0
    end as communication_flag,
    case
        when gross_motor_recommendation is not null
        then 1
        else 0
    end as gross_motor_flag,
    case
        when fine_motor_recommendation is not null
        then 1
        else 0
    end as fine_motor_flag,
    case
        when personal_social_outcome is not null
        then 1
        else 0
    end as personal_social_flag,
    case
        when problem_solving_recommendation is not null
        then 1
        else 0
    end as problem_solving_flag,
    1 as test_domain_count
    from
    directory.asq_test_data_v aatdv
    where organization_id IN ({','.join(['%s'] * len(organization_ids))})) main
    group by test_interval, test_interval_str
    order by test_interval_str"""
    try:
        test_interval_risk_df = pd.read_sql(risk_sql, dbp.create_database_engine(), params=organization_ids)
        return_df = test_interval_risk_df.rename(columns={'test_interval_str': 'test_interval'})
        return return_df.to_dict(orient='records')
    except Exception as e:
        return str(e)

def agency_duplicate_report(organization_id: list):
    organization_ids = tuple(organization_id)
    duplicate_sql = f"""
    with unique_resp as (select agency resp_agency, count(distinct respondent_id) as total_respondent_count
        from directory.asq_test_data_v atdv group by resp_agency) 
        
    select 
            agency,
            sum(diff) duplicate_count,
            count(*) as total_test_count,
            unique_resp.total_respondent_count
            from ( 
            select
            agency,
            case 
                when test_interval - lag(test_interval) over (partition by respondent_id) < 12
                then 1
                else 0
            end
            as diff
            from 
            directory.asq_test_data_v aatdv
            where organization_id IN ({','.join(['%s'] * len(organization_ids))})
            order by respondent_id, test_interval ) main
            join unique_resp on main.agency = unique_resp.resp_agency
            group by agency, total_respondent_count
        """
    try:
        agency_duplicate_df = pd.read_sql(duplicate_sql, dbp.create_database_engine(), params=organization_ids)
        return agency_duplicate_df.to_dict(orient='records')
    except Exception as e:
        return str(e)

def agency_level_scores_by_domain(organization_id: list):
    organization_ids = tuple(organization_id)
    agency_domain_sql = f"""
    select
        agency,
        test_location,
        test_interval_str as test_interval,
        test_type,
        test_score,
        referral_cutoff,
        monitoring_cutoff,
        domain_type
        from directory.agency_level_score_info_v
        where organization_id IN ({','.join(['%s'] * len(organization_ids))})
    """
    try:
        agency_scores_df = pd.read_sql(agency_domain_sql, dbp.create_database_engine(), params=organization_ids)
        return agency_scores_df.to_dict(orient='records')
    except Exception as e:
        return str(e)


def parse_polygon(polygon_str):
    # Remove the outer parentheses
    polygon_str = polygon_str.strip('()')
    
    # Split the string into individual coordinate pairs
    coordinate_pairs = polygon_str.split(', ')
    
    # Parse each coordinate pair into a list of [longitude, latitude]
    coordinates = []
    for pair in coordinate_pairs:
        lon, lat = pair.split()
        coordinates.append([float(lat), float(lon)])
    
    return coordinates

def agency_census_tract_report(organization_id: list):
    organization_ids = tuple(organization_id)
    census_tract_sql = f"""
    select 
        aatdv.census_tract_id,
        coalesce(census_tract_polygon_coordinates, '((0, 0))') as polygon_field,
        agency,
        organization_id,
        count(*) as test_count
        from directory.asq_test_data_v aatdv
        join directory.census_tract_polygons ctp on aatdv.census_tract_id = ctp.census_tract_id
        where organization_id IN ({','.join(['%s'] * len(organization_ids))}) 
        group by
        aatdv.census_tract_id,
        polygon_field,
        agency,
        organization_id
    """
    try:
        agency_census_tract_df = pd.read_sql(census_tract_sql, dbp.create_database_engine(), params=organization_ids)
        agency_census_tract_df['polygon_field'] = agency_census_tract_df['polygon_field'].apply(parse_polygon)
        return agency_census_tract_df.to_dict(orient='records')
    except Exception as e:
        return str(e)

def return_agency_cutoff_report(organization_id: list):
    organization_ids = tuple(organization_id)
    try:
        columns = ', '.join(models.AgencyCutOffReport.model_fields)
        print (columns)
        agency_report_df = pd.read_sql(f"select distinct {columns} from directory.respondent_cutoff_report_v where organization_id IN ({','.join(['%s'] * len(organization_ids))})", dbp.create_database_engine(), params=organization_ids)
        print (agency_report_df)
        return agency_report_df.to_dict(orient='records')
    except Exception as e:
        return str(e)


if __name__ == '__main__':
    #print(return_respondent_cutoff_report(True, ['auth0|675b58eb5cc1d7bd4f246db1']))
    #print(return_respondent_cutoff_report(False, ['auth0|675b58eb5cc1d7bd4f246db1']))
    print(agency_report_filter(['org_8ASAsKKb0MvRv9Sx']))
    #print(test_interval_risk_factors(['auth0|675b58eb5cc1d7bd4f246db1']))
    #print(agency_duplicate_report(['auth0|675b58eb5cc1d7bd4f246db1']))
    #print(len(agency_level_scores_by_domain(['123e4567-e89b-12d3-a456-426614174000'])))
    #print(agency_census_tract_report(['auth0|675b58eb5cc1d7bd4f246db1']))