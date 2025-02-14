from pydantic import BaseModel
from typing import Any, Dict, List, Optional

class MappedDataItem(BaseModel):
    spreadsheetHeader: str
    selectedOption: Optional[str] = None


class MappedData(BaseModel):
    fileName: str
    mappedData: List[MappedDataItem]

#TODO - this should also contain city
class ReconcileSelection(BaseModel):
    partialMatch: bool
    new: bool
    skip: bool
    directoryWrong: bool
    asq_record_address: str
    asq_record_birthdate: str
    asq_record_first_name: str
    asq_record_gender: str
    asq_record_last_name: str
    asq_record_middle_name: str
    asq_record_respondent_id: str
    asq_record_state: str
    asq_record_zip: str
    directory_address: str
    directory_birthdate: str
    directory_first_name: str
    directory_gender: str
    directory_last_name: str
    directory_middle_name: str
    directory_state: str
    directory_zip: str

class ReconcilePayload(BaseModel):
    fileName: str
    reconcileData: List[ReconcileSelection]

class SkipReconcilePayload(BaseModel):
    fileName: str

class JobStatus(BaseModel):
    currentStep: int
    fileName: str
    userId: str
    organizationId: Optional[str]

class DeleteJobRequest(BaseModel):
    job_id: int

class Auth0IDData(BaseModel):
    id_data: List


#Report Models
class AgencyReportFilter(BaseModel):
    agency_filter: List

class RespodentCutOffReport(BaseModel):
    respondent_id: str
    test_type: str
    test_interval: str
    agency: str
    test_location: str
    communication_cutoff_ratio: float
    gross_motor_cutoff_ratio: float
    fine_motor_cutoff_ratio: float
    personal_social_cutoff_ratio: float
    problem_solving_cutoff_ratio: float
    communication_score: int
    gross_motor_score: int
    fine_motor_score: int
    personal_social_score: int
    problem_solving_score: int
    communication_color: str
    gross_motor_color: str
    fine_motor_color: str
    personal_social_color: str
    problem_solving_color: str

class TestIntervalRiskFactorsReport(BaseModel):
    test_interval: str
    communication_risk: int
    gross_motor_risk: int
    fine_motor_risk: int
    personal_social_risk: int
    problem_solving_risk: int
    test_domain_count: int

class AgencyDuplicateReport(BaseModel):
    agency: str
    duplicate_count: int
    total_test_count: int
    total_respondent_count: int

class AgencyScoresByDomainReport(BaseModel):
    agency: str
    test_location: str
    test_interval: str
    test_type: str
    test_score: int
    referral_cutoff: float
    monitoring_cutoff: float
    domain_type: str


class AgencyCensusTractReport(BaseModel):
    agency: str
    census_tract_id: str
    polygon_field: List
    test_count: int

class DomainAveragesReport(BaseModel):
    test_interval: str
    test_location: str
    communication_avg: float
    gross_motor_avg: float
    fine_motor_avg: float
    personal_social_avg: float
    problem_solving_avg: float
    communication_cutoff: float
    communication_near_cutoff: float
    gross_motor_cutoff: float
    gross_motor_near_cutoff: float
    fine_motor_cutoff: float
    fine_motor_near_cutoff: float
    personal_social_cutoff: float
    personal_social_near_cutoff: float
    problem_solving_cutoff: float
    problem_solving_near_cutoff: float

class TestTypeAveragesReport(BaseModel):
    test_count: int
    domain_averages: List[DomainAveragesReport]

class AgencyCutOffReport(BaseModel):
    test_type: str
    test_interval_str: str
    agency: str
    test_location: str
    communication_cutoff_ratio: float
    gross_motor_cutoff_ratio: float
    fine_motor_cutoff_ratio: float
    personal_social_cutoff_ratio: float
    problem_solving_cutoff_ratio: float
    communication_score: int
    gross_motor_score: int
    fine_motor_score: int
    personal_social_score: int
    problem_solving_score: int
    communication_color: str
    gross_motor_color: str
    fine_motor_color: str
    personal_social_color: str
    problem_solving_color: str
