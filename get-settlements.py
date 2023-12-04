import os
from datetime import datetime

import requests
from dotenv import load_dotenv
from smb.SMBConnection import SMBConnection

from credentials import credentials

load_dotenv()

# SMB Constants
SERVER_NAME = os.getenv("NAS_SERVER_NAME")
SHARE_NAME = os.getenv("NAS_SHARE_NAME")
SMB_PATH = os.getenv("NAS_SETTLEMENTS_PATH")
CLIENT_NAME = "local_machine"  # Can be any identifiable string

# Determine the directory of the current script
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

def get_full_path(filename):
    """
    Get the full path of a file or directory based on the script's directory.
    """
    return os.path.join(SCRIPT_DIR, filename)

# --- API FUNCTIONS ---

# Get Access Token
def get_access_token():
    """
    Obtain the access token for API authentication.
    """
    # Fetch the token using the provided credentials
    token_response = requests.post(
        "https://api.amazon.com/auth/o2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": os.getenv("REFRESH_TOKEN"),
            "client_id": os.getenv("LWA_APP_ID"),
            "client_secret": os.getenv("LWA_CLIENT_SECRET"),
        },
    )
    print("Access Token Retrieved")
    return token_response.json()["access_token"]

def get_downloaded_report_ids():
    """
    Fetch the list of already downloaded report IDs.
    """
    # If the settlements-log.txt file doesn't exist, return an empty set.
    log_path = get_full_path("settlements-log.txt")
    if not os.path.exists(log_path):
        return set()

    # If the settlements-log.txt file exists, read it line by line,
    # and each line is considered a report ID.
    with open(log_path, "r") as f:
        return set(f.read().splitlines())


def download_report(report_document_id, report_id, data_start_time, data_end_time, access_token):
    """
    Download the report by its ID and save it to a local file.
    """
    headers = {
        "x-amz-access-token": access_token
    }
    
    # Fetch the report's download information
    response = requests.get(f'https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/documents/{report_document_id}', headers=headers)
    response.raise_for_status()
    
    download_url = response.json()['url']
    
    # Download the report
    report_data = requests.get(download_url)
    report_data.raise_for_status()

    # Parse and format the dates to MM-DD-YYYY
    formatted_start_time = datetime.strptime(data_start_time, "%Y-%m-%dT%H:%M:%S%z").strftime('%m-%d-%Y')
    formatted_end_time = datetime.strptime(data_end_time, "%Y-%m-%dT%H:%M:%S%z").strftime('%m-%d-%Y')

    
    # Ensure the 'settlement-downloads' subfolder exists; if not, create it
    downloads_folder = get_full_path('settlement-downloads')
    if not os.path.exists(downloads_folder):
        os.mkdir(downloads_folder)

    # Save the report data to a file with the desired naming convention inside 'settlement-downloads' subfolder
    filename = os.path.join(downloads_folder, f"disb_{formatted_start_time}_{formatted_end_time}_{report_id}.tsv")
    with open(filename, "wb") as f:
        f.write(report_data.content)
    
    # Upload to SMB share
    smb_upload(filename, f"disb_{formatted_start_time}_{formatted_end_time}_{report_id}.tsv")


    
def log_downloaded_report_id(report_id):
    """
    Log a report ID to the external file.
    """
    # Open the "settlements-log.txt" file in append mode.
    # If the file doesn't exist, it will be created.
    with open(get_full_path("settlements-log.txt"), "a") as f:
        # Write the provided report ID to the file followed by a newline.
        # This newline ensures that each report ID is on a separate line, 
        # making it easier to read and manage the file.
        f.write(report_id + "\n")


def get_settlement_report(access_token):
    """
    Fetch the report for GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2 and download the latest one.
    """
    # Base endpoint for the Selling Partner API
    endpoint = "https://sellingpartnerapi-na.amazon.com"
    
    # Headers required for the API request, including the access token for authentication
    headers = {
        "x-amz-access-token": access_token
    }
    
    # Make a GET request to fetch the specified report type from the API
    report_response = requests.get(
        endpoint + "/reports/2021-06-30/reports?reportTypes=GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2",
        headers=headers
    )
    
    # If the request was successful (HTTP status code 200)
    if report_response.status_code == 200:
        # Parse the JSON response to retrieve report details
        report_data = report_response.json()

        # Fetch the IDs of reports that have already been downloaded to avoid re-downloading
        downloaded_reports = get_downloaded_report_ids()

        # Iterate through the list of available reports
        for report in report_data['reports']:
            # Extract the report ID and its processing status
            report_id = report['reportId']
            processing_status = report['processingStatus']
            data_start_time = report['dataStartTime']
            data_end_time = report['dataEndTime']
            
            # Check if the report has been processed successfully (status is "DONE") 
            # and if it hasn't been downloaded before
            if processing_status == "DONE" and report_id not in downloaded_reports:
                try:
                    report_document_id = report['reportDocumentId']
                    download_report(report_document_id, report_id, data_start_time, data_end_time, access_token)
                    log_downloaded_report_id(report_id)
                except Exception as e:
                    print(f"Error processing report with ID {report_id}: {e}")
   
  
    # If the request was not successful, print the error status code for debugging
    else:
        print(f"Error fetching report. Status code: {report_response.status_code}")

def smb_upload(local_file_path, remote_file_name):
    """
    Upload a local file to the specified SMB share.
    """
    conn = SMBConnection("", "", CLIENT_NAME, SERVER_NAME, use_ntlm_v2=True)
    assert conn.connect(SERVER_NAME)

    with open(local_file_path, 'rb') as file_obj:
        conn.storeFile(SHARE_NAME, SMB_PATH + '/' + remote_file_name, file_obj)

    conn.close()


if __name__ == "__main__":
    token = get_access_token()
    get_settlement_report(token)