import configparser
import datetime
import gzip
import os
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import BytesIO
from os.path import join

import requests
from dotenv import load_dotenv
from smb.SMBConnection import SMBConnection

load_dotenv()


# Load the configuration file
config = configparser.ConfigParser()
config.read(os.getenv("CONFIG_PATH"))

EMAIL_RECIPIENTS = [email.strip() for email in config['EMAIL']['recipients'].split(',')]


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
    print(start_date)
    print(end_date)

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

    return response_data["reportId"], start_date, end_date   # Return the report ID and the date range


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

def test_smb_connection(server_name, share_name, username, password, domain=''):
    try:
        # Create a connection object
        conn = SMBConnection(
            username,          # UserID
            password,          # Password
            "client_machine",  # Client machine name
            server_name,       # Server name
            domain=domain,     # Domain name
            use_ntlm_v2=True,  # Recommended to set this as True
            is_direct_tcp=True # Should be True for SMB over direct IP (port 445), False for NetBIOS (port 139)
        )
        
        # Connect to server
        connected = conn.connect(server_name, 445)  # 445 is for direct SMB over IP, change to 139 for NetBIOS if necessary
        if connected:
            print(f"Successfully connected to {server_name}")
            
            # List shared folders
            shares = conn.listShares()
            for share in shares:
                print(f"- {share.name}")
        else:
            print(f"Failed to connect to {server_name}")
    except Exception as e:
        print(f"Error: {e}")


def save_to_tsv(report_content, server_name, share_name, smb_path, filename, username, password, domain=''):
    conn = SMBConnection(username, password, "client_machine", server_name, domain=domain, use_ntlm_v2=True, is_direct_tcp=True)
    
    if not conn.connect(server_name, 445):
        raise ConnectionError(f"Unable to connect to the server: {server_name}")

    # Using BytesIO to create a file-like object in memory and then upload it to the SMB share
    with BytesIO(report_content.encode('utf-8')) as file:
        conn.storeFile(share_name, join(smb_path, filename), file)
    
    print(f"Report saved as: {filename}")


def send_email():
    # Send email
    sender_email = os.getenv("SMTP_SENDER_EMAIL")
    sender_password = os.getenv("SMTP_SENDER_PASSWROD")

    subject = "Daily Inv Ledger: amazonia.tsv Uploaded to AS400"
    body = f"filename: amazonia.tsv\nhas been uploaded and is ready for processing."
    recipients = EMAIL_RECIPIENTS

    # Set up the MIME
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = ", ".join(recipients)
    message["Subject"] = subject
    message.attach(MIMEText(body, 'plain'))

    # Connect and send the email
    try:
        server = smtplib.SMTP(os.getenv("SMTP_SERVER"), 587)
        server.starttls()  # Encrypts the connection
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipients, message.as_string())
        server.close()

        #print("Email sent successfully")
    except Exception as e:
        print(f"Error sending email: {e}")


# --- MAIN EXECUTION ---

if __name__ == "__main__":
    # Modify these variables accordingly for SMB connection
    SERVER_NAME = os.getenv("IBM_SERVER_NAME")
    SHARE_NAME = os.getenv("IBM_SHARE_NAME")
    USERNAME = os.getenv("IBM_USERNAME")
    PASSWORD = os.getenv("IBM_PASSWORD")
    DOMAIN = ''
    
    # Test the SMB connection
    test_smb_connection(SERVER_NAME, SHARE_NAME, USERNAME, PASSWORD, DOMAIN)

    # Get the Amazon access token
    access_token = get_access_token()

    # Create a report and get its ID and date range
    report_id, start_date, end_date = create_report(access_token)

    # Wait for the report to be ready
    document_id = poll_report_status(access_token, report_id)

    # Download the report content
    report_content = download_report(access_token, document_id)

    # Save the report to the remote SMB server as "amazonia.tsv"
    save_to_tsv(report_content, SERVER_NAME, SHARE_NAME, '', 'amazonia.tsv', USERNAME, PASSWORD, DOMAIN)

    # Send email alerts
    send_email()