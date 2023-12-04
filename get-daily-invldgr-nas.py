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

def get_access_token():
    """
    Obtain the access token for authenticating with the API.
    """
    # Request token from Amazon
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


def create_report(access_token):
    """
    Create a report with the start date being 3 days prior to the current day
    and the end date being the current day.
    """
    endpoint = "https://sellingpartnerapi-na.amazon.com"
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }

    # Calculate the date range
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    start_date = end_date - datetime.timedelta(days=1)
    # print(start_date)
    # print(end_date)

    print(f"Report Date Range:\nStart Date: {start_date}\nEnd Date: {end_date}")

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

    # Handle response and potential errors
    if report_creation_response.status_code == 202:
        print("Report creation started successfully and is being processed.")
    elif report_creation_response.status_code != 200:
        print(f"Error creating report. Status code: {report_creation_response.status_code}")
        print(report_creation_response.text)
        report_creation_response.raise_for_status()

    response_data = report_creation_response.json()
    if "reportId" not in response_data:
        print("Unexpected response:")
        print(response_data)
        raise ValueError("Response did not contain 'reportId'")
    print(f"Report Creation Response: {response_data}")

    return response_data["reportId"], start_date, end_date  # Return the report ID and the date range


def poll_report_status(access_token, report_id):
    """
    Poll the status of a report using its report ID until its status is "DONE".
    If the report processing fails, raise an exception.
    """
    endpoint = "https://sellingpartnerapi-na.amazon.com"
    headers = {
        "x-amz-access-token": access_token
    }

    print(f"Polling report with ID: {report_id}")
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
        time.sleep(60)  # Check report status every minute


def download_report(access_token, document_id):
    """
    Download the report's contents using its document ID.
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

    # Decompress the report's contents
    buffer = BytesIO(compressed_content)
    with gzip.GzipFile(fileobj=buffer, mode='rb') as f:
        report_content = f.read().decode('utf-8')
    print(f"Downloaded Report with Document ID: {document_id}")

    return report_content


# --- SMB FUNCTIONS ---

def test_smb_connection(server_name, share_name):
    try:
        conn = SMBConnection("", "", "client_machine", server_name, use_ntlm_v2=True, is_direct_tcp=True)

        if not conn.connect(server_name, 445):
            print(f"Failed to connect to {server_name}")
            return

        print(f"Successfully connected to {server_name}")

        # List shared folders
        shares = conn.listShares()
        for share in shares:
            print(f"- {share.name}")
    except Exception as e:
        print(f"Error: {e}")


def save_to_tsv(report_content, server_name, share_name, smb_path, filename):
    conn = SMBConnection("", "", "client_machine", server_name, use_ntlm_v2=True, is_direct_tcp=True)

    if not conn.connect(server_name, 445):
        raise ConnectionError(f"Unable to connect to the server: {server_name}")

    with BytesIO(report_content.encode('utf-8')) as file:
        conn.storeFile(share_name, join(smb_path, filename), file)

    print(f"Report saved as: {filename}")


def week_of_month(dt):
    """
    Calculate the week of the month for a specific date.
    """
    first_day = dt.replace(day=1)
    dom = first_day.weekday()  # Day of the month for the 1st of the month
    adjusted_dom = dt.day + dom
    week_num = (adjusted_dom - 1) // 7 + 1
    return week_num


# --- MAIN EXECUTION ---

if __name__ == "__main__":
    # Modify these variables accordingly for SMB connection
    SERVER_NAME = os.getenv("NAS_SERVER_NAME")
    SHARE_NAME = os.getenv("NAS_SHARE_NAME")
    SMB_PATH = os.getenv("NAS_DAILY_LEDGER_PATH")

    # Test the SMB connection
    test_smb_connection(SERVER_NAME, SHARE_NAME)

    # Get the Amazon access token
    access_token = get_access_token()

    # Create a report and get its ID and date range
    report_id, start_date, end_date = create_report(access_token)

    # Calculate filename based on 2 days prior to the current date
    date_two_days_prior = datetime.date.today() - datetime.timedelta(days=2)
    date_str = date_two_days_prior.strftime('%m-%d-%Y')  # Format: MM-DD-YYYY
    filename = f"amazonia_{date_str}.tsv"

    # Wait for the report to be ready
    document_id = poll_report_status(access_token, report_id)

    # Download the report content
    report_content = download_report(access_token, document_id)

    # Save the report to the specified SMB server with the calculated filename
    save_to_tsv(report_content, SERVER_NAME, SHARE_NAME, SMB_PATH, filename)