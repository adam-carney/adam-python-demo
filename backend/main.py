from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os
import models
import database_processing as dbp
import file_processing as fp
import data_processing as dp
import process_dml_functions as dml
import requests
import return_data_visualization_data as rdvd
import authorization_processing as ap

load_dotenv()

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

@app.get("/")
async def read_root():
    return {"Base Services are up and running"}


@app.post("/store-job-status/")
def store_job_status(job_status: models.JobStatus):
    try:
        store_job_status = job_status.model_dump()

        result = dbp.insert_job_status(
            store_job_status["currentStep"],
            store_job_status["fileName"],
            store_job_status["userId"],
            store_job_status["organizationId"],
        )

        # Return a success response
        return {"msg": "Job state stored successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/upload-file/")
async def create_upload_file(file: UploadFile = File(...)):
    try:
        return_dict = await fp.upload_file_processing(file)
        return return_dict
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download-file/")
def get_file_from_blob_store(filename: str = Query(..., description="The name of the file to download")):
    try:
        dbp.fetch_schema_headers_from_db()
        downloaded_blob_properties = fp.download_file_processing(filename)
        #print(downloaded_blob_properties['downloaded_blob'])
        return_dict = dp.handle_file(downloaded_blob_properties['downloaded_blob'], downloaded_blob_properties['content_type'])
        print (return_dict)
        return return_dict
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/asq-test-detail-columns/")
def get_asq_test_detail_columns():
    try:
        asq_test_detail_columns = dbp.fetch_init_file_headers()
        return asq_test_detail_columns
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/mapping-submit/")
def mapping_submit(mapped_data: models.MappedData):
    try:
        file_name = mapped_data.fileName
        mapped_data_dict = mapped_data.model_dump()
        mapped_data_items = mapped_data_dict['mappedData']
        rename_dict = {item['spreadsheetHeader']: item['selectedOption'] for item in mapped_data_items}
        print(f'name of thing file_name: {type(file_name)} {file_name}')
        print(f'name of thing mapped_data_dict: {type(mapped_data_dict)} {mapped_data_dict}')
        print(f'name of thing mapped_data_items: {type(mapped_data_items)} {mapped_data_items}')
        print(f'name of thing rename_dict: {type(rename_dict)} {rename_dict}')
        downloaded_blob_properties = fp.download_file_processing(file_name)
        return_data = dp.process_mapping_submission(downloaded_blob_properties, rename_dict, file_name)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"status": "success", "data": return_data}

@app.get("/partial-matches/")
def process_partial_matches(file_name: str= Query(..., alias="file_name")):
    print(f'Fetching partial matches for {file_name}')
    return_dict = {
        'partials_data': [],
        'errors': [],
    }

    return_data = dp.process_records_matching(file_name)
    return_dict['partials_data'] = return_data

    return_dict = return_data
    return return_dict


@app.post("/reconcile-submit/")
def reconcile_submit(payload: models.ReconcilePayload):
    """
    Pydantic
    """
    payload_dict = payload.model_dump()
    reconcile_list = []
    print(f'payload dict {type(payload_dict)} {payload_dict}')
    try:
        for selection in payload_dict['reconcileData']:
            print(f'selection partialMatch{type(selection["partialMatch"])} {selection["partialMatch"]}')
            print(f'selection new {type(selection["new"])} {selection["new"]}')
            print(f'selection skip {type(selection["skip"])} {selection["skip"]}')
            print(f'selection directoryWrong {type(selection["directoryWrong"])} {selection["directoryWrong"]}')
            reconcile_type = ''
            if selection['partialMatch'] is True:
                reconcile_type = 'PARTIAL_MATCH'
            if selection['new'] is True:
                reconcile_type = 'NEW'
            if selection['skip'] is True:
                reconcile_type = 'SKIP'
            if selection['directoryWrong'] is True:
                reconcile_type = 'DIRECTORY_WRONG'
            print(f'reconcile type {type(reconcile_type)} {reconcile_type}')
            reconcile_list.append(
                {
                    "asq_record_respondent_id": selection['asq_record_respondent_id'],
                    "reconcile_type": reconcile_type
                }
            )
        print(f'Reconcile list: {type(reconcile_list)}, {reconcile_list}')
        fileName = payload_dict['fileName']
        print(f'fileName: {type(fileName)}, {fileName}')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


    result = dbp.update_reconciled_status(reconcile_list, fileName)
    #Update remaining records to NEW
    dbp.update_new_respondents(fileName)

    dml.process_asq_data_updates(fileName)


    return {"message": "Reconcile data processed successfully", "data": result}


@app.post("/skip-reconcile-submit/")
def skip_reconcile_submit(payload: models.SkipReconcilePayload):
    """
    Pydantic
    """
    payload_dict = payload.model_dump()
    print(f'payload dict {type(payload_dict)} {payload_dict}')
    file_name = payload_dict['fileName']
    try:
        #Update new records to NEW
        dbp.update_new_respondents(file_name)
        dml.process_asq_data_updates(file_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"data": "Data processed successfully"}


@app.get("/my-jobs/")
def fetch_my_jobs(user_id: str= Query(...)):
    print(f'Fetching jobs for user_id: {user_id}')
    return_dict = {
        'my_jobs': [],
        'organization_display_names': [],
        'user_display_names': [],
        'errors': [],
    }
    user_id_list = []

    auth0_id_data= ap.get_auth_id_data(user_id)
    print(f'Auth0 ID data: {auth0_id_data}')
    # [
    #         {
    #             "id": "auth0|675b58eb5cc1d7bd4f246db1",
    #             "name": "Chris Hionis",
    #             "id_type": "user"
    #         },
    #         {
    #             "id": "auth0|676869d66a0a0ae37ab1574d",
    #             "name": "Adam Carney",
    #             "id_type": "user"
    #         },
    #         {
    #             "id": "org_8ASAsKKb0MvRv9Sx",
    #             "name": "Brightpath Dev",
    #             "id_type": "organization"
    #         },
    #         {
    #             "id": "org_odotXNJNyhafRy9Z",
    #             "name": "MC Super Entity",
    #             "id_type": "organization"
    #         }
    #     ]

    # Getting all jobs by all users
    for item in auth0_id_data:
        if item['id_type'] == 'user':
            user_id_list.append(item['id'])
    return_data_df = dbp.fetch_my_jobs(user_id_list)
    return_data_list_of_dicts = return_data_df.to_dict(orient='records')
    return_dict['my_jobs'] = return_data_list_of_dicts

    # Building organization and user display names
    organization_display_names = {item['id']: item['name'] for item in auth0_id_data if item['id_type'] == 'organization'}
    user_display_names = {item['id']: item['name'] for item in auth0_id_data if item['id_type'] == 'user'}
    return_dict['organization_display_names'] = organization_display_names
    return_dict['user_display_names'] = user_display_names

    return return_dict


@app.post("/delete-job/")
def delete_job(request: models.DeleteJobRequest):
    """
    User has clicked the gargage can icon on one of their jobs.
    We are going to "delete" the job by updating the job status to zero (0)
    while leaving everything else in tact.
    """
    job_id = request.job_id
    results = dbp.archive_job(job_id)
    if results['status'] == 'error':
        raise HTTPException(status_code=500, detail=results['message'])
    if results['status'] == 'success':
        return {"message": "Job deleted successfully"}


@app.get("/respondent-match-statuses/")
def fetch_respondent_match_statuses(file_name: str= Query(...)):
    print(f'Fetching matching respondents for job file: {file_name}')
    return_dict = {
        'respondents_matching_statuses': [],
        'errors': [],
    }

    return_data_df = dbp.fetch_respondent_matches(file_name)
    return_data_list_of_dicts = return_data_df.to_dict(orient='records')

    return_dict['respondents_matching_statuses'] = return_data_list_of_dicts

    return return_dict


#FrontEnd Auth0 Endpoints
@app.get("/auth-id-data/")
def fetch_auth0_id_data(userId: str= Query(...)):
    print(f'Fetching Auth0 ID data for user_id: {userId}')
    return_dict = {
        'auth0_id_data': [],
        'errors': [],
    }

    return_data = ap.get_auth_id_data(userId)
    return_dict = models.Auth0IDData(id_data=return_data)
    return return_dict


### Data Visualization Endpoints ###
@app.get("/fetch-directory/")
def fetch_directory():
    print(f'Fetching directory...')
    return_dict = {
        'directory': [],
        'errors': [],
    }

    return_data_df = dbp.fetch_directory()

    return_data_list_of_dicts = return_data_df.to_dict(orient='records')

    return_dict['directory'] = return_data_list_of_dicts

    return return_dict

@app.get("/fetch-agency-duplicate-report/")
def fetch_agency_duplicate_report(userId: str= Query(...)):
    print(f'Fetching agency duplicate report...')
    organization_id = ap.return_orgs_by_user(userId)
    agency_data = rdvd.agency_duplicate_report(organization_id=organization_id)
    return_dict = [models.AgencyDuplicateReport(**data) for data in agency_data]
    return return_dict

@app.get("/fetch-agency-report-filter/")
def fetch_agency_report_filter(userId: str= Query(...)):
    print(f'Fetching agency report filter...')
    organization_id = ap.return_orgs_by_user(userId)
    agency_data = rdvd.agency_report_filter(organization_id=organization_id)

    return_dict = [models.AgencyReportFilter(**data) for data in agency_data]

    return return_dict

@app.get("/fetch-respondent-cutoff-report/")
def fetch_respondent_cutoff_report():
    print(f'Fetching respondent cutoff report...')
    organization_id = ['auth0|675b58eb5cc1d7bd4f246db1']
    respondent_data = rdvd.return_respondent_cutoff_report(True, organization_id=organization_id)
    return_dict = [models.RespodentCutOffReport(**data) for data in respondent_data]
    return return_dict

@app.get("/fetch-agency-scores-by-domain-report/")
def fetch_agency_scores_by_domain_report(userId: str= Query(...)):
    print(f'Fetching agency scores by domain report...')
    organization_id = ap.return_orgs_by_user(userId)
    print(f'Organization ID: {organization_id}')
    agency_scores_data = rdvd.agency_level_scores_by_domain(organization_id=organization_id)
    return_dict = [models.AgencyScoresByDomainReport(**data) for data in agency_scores_data]
    return return_dict


@app.get("/fetch-agency-census-tract-report/")
def fetch_agency_census_tract_report(userId: str= Query(...)):
    print(f'Fetching agency census tract report...')
    organization_id = ap.return_orgs_by_user(userId)
    census_tract_data = rdvd.agency_census_tract_report(organization_id=organization_id)
    for data in census_tract_data:
        print(data)
    return_dict = [models.AgencyCensusTractReport(**data) for data in census_tract_data]
    return return_dict


@app.get("/fetch-test-interval-risk-factors-report/")
def fetch_test_interval_risk_factors_report(userId: str= Query(...)):
    print(f'Fetching test interval risk factors report...')
    organization_id = ap.return_orgs_by_user(userId)
    test_interval_data = rdvd.test_interval_risk_factors(organization_id=organization_id)
    return_dict = [models.TestIntervalRiskFactorsReport(**data) for data in test_interval_data]
    return return_dict

@app.get("/fetch-agency-cutoff-report/")
def fetch_respondent_cutoff_report(userId: str= Query(...)):
    print(f'Fetching respondent cutoff report...')
    organization_id = ap.return_orgs_by_user(userId)
    agency_data = rdvd.return_agency_cutoff_report(organization_id=organization_id)
    return_dict = [models.AgencyCutOffReport(**data) for data in agency_data]
    return return_dict