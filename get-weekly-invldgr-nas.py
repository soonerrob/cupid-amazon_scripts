import datetime
import gzip
import time
from io import BytesIO
from os.path import join

import requests
from smb.SMBConnection import SMBConnection

from credentials import credentials

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
            "refresh_token": credentials["refresh_token"],
            "client_id": credentials["lwa_app_id"],
            "client_secret": credentials["lwa_client_secret"],
        },
    )
    print("Access Token Retrieved")
    return token_response.json()["access_token"]


def create_report(access_token):
    """
    Create a report based on a given date range (from last Tuesday to current Monday).
    If the range crosses two months, adjust the end_date to the last day of the start month.
    """
    endpoint = "https://sellingpartnerapi-na.amazon.com"
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }

    # Determine the range from last Tuesday to current Monday
    today = datetime.date.today()
    end_date = today
    while end_date.weekday() != 0:  # 0 is Monday
        end_date -= datetime.timedelta(days=1)
    start_date = end_date - datetime.timedelta(days=6)

    # Adjust the date range if it spans two months
    if start_date.month != end_date.month:
        end_date = start_date.replace(day=1) - datetime.timedelta(days=1)
    end_date += datetime.timedelta(days=1)  # Include the Monday in the range

    print(f"Weekly Report:\nStart Date: {start_date}\nEnd Date: {end_date}")

    # Set up payload and request report creation
    payload = {
        "reportType": "GET_LEDGER_SUMMARY_VIEW_DATA",
        "dataStartTime": start_date.isoformat(),
        "dataEndTime": end_date.isoformat(),
        "marketplaceIds": ["ATVPDKIKX0DER"],
        "reportOptions": {
            "aggregateByLocation": "FC",  # Aggregate by FC
            "aggregatedByTimePeriod": "DAILY"  # Aggregate daily
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
    SERVER_NAME = "nas-bw-02.cfinc.com"
    SHARE_NAME = "Filestore_NC"
    SMB_PATH = "Amazon Downloads\Weekly Inventory Ledger"
    
    # Test the SMB connection
    test_smb_connection(SERVER_NAME, SHARE_NAME)

    # Get the Amazon access token
    access_token = get_access_token()

    # Create a report and get its ID and date range
    report_id, start_date, end_date = create_report(access_token)

    # Calculate filename based on the start date
    week_num = week_of_month(start_date)
    month_year_str = start_date.strftime('%m-%Y')
    filename = f"amazonia_week{week_num}_{month_year_str}.tsv"

    # Wait for the report to be ready
    document_id = poll_report_status(access_token, report_id)

    # Download the report content
    report_content = download_report(access_token, document_id)

    # Save the report to the specified SMB server with the calculated filename
    save_to_tsv(report_content, SERVER_NAME, SHARE_NAME, SMB_PATH, filename)