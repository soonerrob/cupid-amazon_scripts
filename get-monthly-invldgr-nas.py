import datetime
import gzip
import os
import time
from io import BytesIO
from os.path import join

import requests
from dotenv import load_dotenv
from smb.SMBConnection import SMBConnection

from credentials import credentials

load_dotenv()

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

# Create Report
def create_report(access_token):
    """
    Request to create a new report for the previous month.
    """
    endpoint = "https://sellingpartnerapi-na.amazon.com"
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }

    # Determine the range for the last month
    first_day_of_current_month = datetime.datetime.now().date().replace(day=1)
    last_day_of_last_month = first_day_of_current_month - datetime.timedelta(days=1)
    start_date = last_day_of_last_month.replace(day=1)
    end_date = last_day_of_last_month + datetime.timedelta(days=1)  # Include the last day in the range

    print(f"Monthly Start Date: {start_date}")
    print(f"Monthly End Date: {end_date}")

    # Set up payload and request report creation
    payload = {
        "reportType": "GET_LEDGER_SUMMARY_VIEW_DATA",
        "dataStartTime": start_date.isoformat(),
        "dataEndTime": end_date.isoformat(),
        "marketplaceIds": ["ATVPDKIKX0DER"],
        "reportOptions": {
            "aggregateByLocation": "FC",
            "aggregatedByTimePeriod": "DAILY"
        }
    }
    report_creation_response = requests.post(
        endpoint + "/reports/2021-06-30/reports",
        headers=headers,
        json=payload
    )

    # Handle report creation response
    if report_creation_response.status_code == 202:
        print("Report creation started successfully and is being processed.")
    elif report_creation_response.status_code != 200:
        print(f"Error creating report. Status code: {report_creation_response.status_code}")
        print(report_creation_response.text)  # Display additional details about the error
        report_creation_response.raise_for_status()

    response_data = report_creation_response.json()
    if "reportId" not in response_data:
        print("Unexpected response:")
        print(response_data)
        raise ValueError("Response did not contain 'reportId'")
    print(f"Report Creation Response: {response_data}")

    return response_data["reportId"]

# Poll for Results
def poll_report_status(access_token, report_id):
    """
    Continuously check the status of a report until it's completed.
    """
    endpoint = "https://sellingpartnerapi-na.amazon.com"
    headers = {
        "x-amz-access-token": access_token
    }
    print(f"Polling report with ID: {report_id}")

    # Keep checking the report status every minute until it's done
    while True:
        status_response = requests.get(
            endpoint + f"/reports/2021-06-30/reports/{report_id}",
            headers=headers
        )
        status_data = status_response.json()
        report_status = status_data["processingStatus"]

        if report_status == "DONE":
            return status_data["reportDocumentId"]
        elif report_status in ["CANCELLED", "FAILED"]:
            raise Exception(f"Report processing failed with status {report_status}")

        print(f"Report Status: {report_status}")
        time.sleep(60)  # Wait for a minute before polling again

# Download Report
def download_report(access_token, document_id):
    """
    Fetch the content of the report using its document ID.
    """
    endpoint = "https://sellingpartnerapi-na.amazon.com"
    headers = {
        "x-amz-access-token": access_token
    }
    document_response = requests.get(
        endpoint + f"/reports/2021-06-30/documents/{document_id}",
        headers=headers
    )
    download_url = document_response.json()["url"]
    compressed_content = requests.get(download_url).content

    # Decompress the report's content
    buffer = BytesIO(compressed_content)
    with gzip.GzipFile(fileobj=buffer, mode='rb') as f:
        report_content = f.read().decode('utf-8')
    print(f"Downloaded Report with Document ID: {document_id}")

    return report_content

# --- SMB FUNCTIONS ---

def save_to_tsv(report_content, server_name, share_name, smb_path, filename):
    conn = SMBConnection("", "", "client_machine", server_name, use_ntlm_v2=True, is_direct_tcp=True)
    
    if not conn.connect(server_name, 445):
        raise ConnectionError(f"Unable to connect to the server: {server_name}")

    with BytesIO(report_content.encode('utf-8')) as file:
        conn.storeFile(share_name, join(smb_path, filename), file)
    
    print(f"Report saved as: {filename}")

# --- MAIN EXECUTION ---

if __name__ == "__main__":
    access_token = get_access_token()
    report_id = create_report(access_token)
    document_id = poll_report_status(access_token, report_id)
    report_content = download_report(access_token, document_id)

    # Construct the filename for the previous month
    last_month = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
    month_year_str = last_month.strftime("%m-%Y")
    filename = f"amazonia_{month_year_str}.tsv"

    # Save to SMB share
    server_name = os.getenv("NAS_SERVER_NAME")
    share_name = os.getenv("NAS_SHARE_NAME")
    smb_path = os.getenv("NAS_MONTHLY_LEDGER_PATH")
    save_to_tsv(report_content, server_name, share_name, smb_path, filename)
