import csv
import os

import requests
from dotenv import load_dotenv
from smb.SMBConnection import SMBConnection

from credentials import credentials

load_dotenv()

# Get the base directory of the script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_URL = 'https://sellingpartnerapi-na.amazon.com/fba/inbound/v0/shipments/'

# Constants for the SMB server
SERVER_NAME = os.getenv("NAS_SERVER_NAME")
SHARE_NAME = os.getenv("NAS_SHARE_NAME")
SMB_PATH = os.getenv("NAS_SHIPPMENTS_PATH")
CLIENT_NAME = "local_machine"


def get_access_token():
    """
    Obtain the access token for API authentication.
    """
    print("Attempting to obtain access token...")
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
    log_file = os.path.join(BASE_DIR, "shipments-log.txt")  # Adjusted path
    if not os.path.exists(log_file):
        return set()

    # If the settlements-log.txt file exists, read it line by line,
    # and each line is considered a report ID.
    with open(log_file, "r") as f:
        return set(f.read().splitlines())


def log_downloaded_report_id(report_id):
    """
    Log a report ID to the external file.
    """
    # Open the "shipments-log.txt" file in append mode.
    # If the file doesn't exist, it will be created.
    log_file = os.path.join(BASE_DIR, "shipments-log.txt")  # Adjusted path
    with open(log_file, "a") as f:
        f.write(report_id + "\n")


def get_shipment_ids(access_token):
    """
    Fetch shipment IDs.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-amz-access-token": access_token,
        "User-Agent": "CupidAPI/1.0",
    }

    params = {
        "ShipmentStatusList": "WORKING,READY_TO_SHIP",  # Include both statuses
    }

    response = requests.get(BASE_URL, headers=headers, params=params)

    if response.status_code != 200:
        print(response.json())
        response.raise_for_status()

    shipments = response.json()

    # Adjusting the shipment_ids extraction based on the provided structure
    shipment_data = shipments.get("payload", {}).get("ShipmentData", [])
    shipment_ids = [shipment["ShipmentId"] for shipment in shipment_data]

    return shipment_ids


def get_shipment_items(access_token, shipment_id):
    """
    Fetch items for a specific shipment ID.
    """
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-amz-access-token": access_token,
        "User-Agent": "CupidAPI/1.0",
    }

    # Construct the URL for fetching items of a specific shipment ID
    url = f"{BASE_URL}{shipment_id}/items"

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(response.json())
        response.raise_for_status()

    return response.json()


def save_to_smb(local_file_path, smb_file_name):
    """
    Save a local file to the specified SMB location.
    """
    # Establish a connection to the SMB server without authentication
    smb_conn = SMBConnection(
        "", "", CLIENT_NAME, SERVER_NAME, use_ntlm_v2=True, is_direct_tcp=True)
    smb_conn.connect(SERVER_NAME)

    with open(local_file_path, 'rb') as file:
        smb_conn.storeFile(SHARE_NAME, os.path.join(
            SMB_PATH, smb_file_name), file)

    # Close the connection
    smb_conn.close()

# def save_to_tsv(data, shipment_id, folder_name="shipment-downloads", filename_prefix="shipment"):
#     """
#     Save the provided data to a TSV file inside the specified folder, with the shipment ID appended.
#     """
#     # Ensure the directory exists
#     folder_path = os.path.join(BASE_DIR, folder_name)  # Adjusted path
#     if not os.path.exists(folder_path):
#         os.makedirs(folder_path)

#     filename = f"{filename_prefix}_{shipment_id}.tsv"
#     file_path = os.path.join(folder_path, filename)

#     with open(file_path, mode='w', newline='', encoding='utf-8') as file:
#         writer = csv.writer(file, delimiter='\t')

#         # Writing headers to the TSV
#         headers = ['ShipmentId', 'SellerSKU', 'FulfillmentNetworkSKU', 'QuantityShipped', 'QuantityReceived', 'QuantityInCase', 'PrepInstruction', 'PrepOwner']
#         writer.writerow(headers)

#         # Iterating through each item and writing to the TSV
#         for item in data:
#             prep_details = item['PrepDetailsList'][0] if item.get('PrepDetailsList') else {}
#             writer.writerow([
#                 item.get('ShipmentId', ''),
#                 item.get('SellerSKU', ''),
#                 item.get('FulfillmentNetworkSKU', ''),
#                 item.get('QuantityShipped', ''),
#                 item.get('QuantityReceived', ''),
#                 item.get('QuantityInCase', ''),
#                 prep_details.get('PrepInstruction', ''),
#                 prep_details.get('PrepOwner', '')
#             ])


def save_to_csv(data, shipment_id, folder_name="shipment-downloads", filename_prefix="shipment"):
    """
    Save the provided data to a CSV file inside the specified folder, with the shipment ID appended.
    """
    folder_path = os.path.join(BASE_DIR, folder_name)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    filename = f"{filename_prefix}_{shipment_id}.csv"
    file_path = os.path.join(folder_path, filename)

    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=',')
        headers = ['ShipmentId', 'SellerSKU', 'FulfillmentNetworkSKU', 'QuantityShipped',
                   'QuantityReceived', 'QuantityInCase', 'PrepInstruction', 'PrepOwner']
        writer.writerow(headers)

        for item in data:
            prep_details = item['PrepDetailsList'][0] if item.get(
                'PrepDetailsList') else {}
            writer.writerow([
                item.get('ShipmentId', ''),
                item.get('SellerSKU', ''),
                item.get('FulfillmentNetworkSKU', ''),
                item.get('QuantityShipped', ''),
                item.get('QuantityReceived', ''),
                item.get('QuantityInCase', ''),
                prep_details.get('PrepInstruction', ''),
                prep_details.get('PrepOwner', '')
            ])

    save_to_smb(file_path, filename)

    # Saving to SMB location after saving it locally
    save_to_smb(file_path, filename)


def main():
    access_token = get_access_token()

    # Fetching the list of shipment IDs that were already downloaded
    downloaded_shipment_ids = get_downloaded_report_ids()

    shipment_ids = get_shipment_ids(access_token)

    for shipment_id in shipment_ids:
        if shipment_id not in downloaded_shipment_ids:
            items = get_shipment_items(access_token, shipment_id)
            save_to_csv(items.get('payload', {}).get(
                'ItemData', []), shipment_id)
            log_downloaded_report_id(shipment_id)


if __name__ == "__main__":
    main()
