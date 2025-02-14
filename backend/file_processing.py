from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContentSettings
import base64
import os
import sys
import io
from dotenv import load_dotenv
from io import BytesIO
from pathlib import Path
from datetime import datetime
import uuid

load_dotenv()
account_url = os.getenv('AZURE_STORAGE_URL')
default_credential = DefaultAzureCredential()
container_name = os.getenv('FILE_UPLOAD_CONTAINER_NAME')


async def upload_file_processing(file):
    return_dict = {
        'filename': '',
        'errors': [],
    }
    # Determine filetype from the content_type attribute of the UploadFile object and assign that filetype to Azure Blob Storage's content_settings
    incoming_mime_type = file.content_type
    print(f"The MIME type of the uploaded file is: {type(incoming_mime_type)} {incoming_mime_type}")
    content_settings = ContentSettings(content_type=incoming_mime_type)
    print(f'content_settings {content_settings}')

    # Create unique filename appendages and filename
    original_path = Path(file.filename)
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    unique_id_partial = str(uuid.uuid4()).split('-')[0]
    filename = f"{original_path.stem}_{timestamp}_{unique_id_partial}{original_path.suffix}"

    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient(account_url, credential=default_credential)

    # Create a blob client
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)

    # Assuming `file` is an async file-like object opened for reading ('rb')
    await file.seek(0)  # Move to the start of the file
    file_contents = await file.read()  # Read the contents into memory
    input_stream = io.BytesIO(file_contents)  # Create a BytesIO object with the contents

    try:
        # Upload the file object
        blob_upload_results = blob_client.upload_blob(input_stream, content_settings=content_settings)
        print(f'blob upload results {type(blob_upload_results)} {blob_upload_results}')
        blob_upload_results_dict = {
            'etag': blob_upload_results['etag'],
            'last_modified': blob_upload_results['last_modified'].isoformat() if isinstance(blob_upload_results['last_modified'], datetime) else blob_upload_results['last_modified'],
            'content_md5': base64.b64encode(blob_upload_results['content_md5']).decode('utf-8') if isinstance(blob_upload_results['content_md5'], bytearray) else blob_upload_results['content_md5'],
            'client_request_id': blob_upload_results['client_request_id'],
            'request_id': blob_upload_results['request_id'],
            'version': blob_upload_results['version'],
            'version_id': blob_upload_results['version_id'],
            'date': blob_upload_results['date'].isoformat() if isinstance(blob_upload_results['date'], datetime) else blob_upload_results['date'],
            'request_server_encrypted': blob_upload_results['request_server_encrypted'],
            'encryption_key_sha256': blob_upload_results['encryption_key_sha256'],
            'encryption_scope': blob_upload_results['encryption_scope']
        }
        print(f'blob upload results dict {type(blob_upload_results_dict)} {blob_upload_results_dict}')
        if 'etag' in blob_upload_results_dict and 'last_modified' in blob_upload_results_dict:
            return_dict['success'] = True
        else:
            return_dict['success'] = False

    except Exception as e:
        print(f'An error occurred while uploading the file: {e}')
        return_dict['success'] = False
        return_dict['error'] = [str(e)]

    return_dict['filename'] = filename
    # return_dict['filestore_info'] = blob_upload_results_dict

    return return_dict

def download_file_processing(filename):
    blob_service_client = BlobServiceClient(account_url, credential=default_credential)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
    blob_properties = blob_client.get_blob_properties()
    content_type = blob_properties.content_settings.content_type
    downloaded_blob = blob_client.download_blob().readall()
    return_dict = {
        'downloaded_blob': downloaded_blob,
        'content_type': content_type
    }
    return return_dict

if __name__ == '__main__':
    print(type(download_file_processing('asq_test_scenarios_2_20241126153811_f5a3a952.xlsx')))