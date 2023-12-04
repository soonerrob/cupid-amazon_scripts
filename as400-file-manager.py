#!/usr/bin/env python3

import configparser
import os
import shutil
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from dotenv import load_dotenv

load_dotenv()



from smb.SMBConnection import SMBConnection

# Load the configuration file
config = configparser.ConfigParser()
config.read(os.getenv("CONFIG_PATH"))

os.getenv("REFRESH_TOKEN")

# Constants
SERVER_NAME = os.getenv("IBM_SERVER_NAME")
SHARE_NAME = os.getenv("IBM_SHARE_NAME")
USERNAME = os.getenv("IBM_USERNAME")
PASSWORD = os.getenv("IBM_PASSWORD")
DOMAIN = ''
PORT = 445


# Extract values from the configuration file
CHECK_INTERVAL = int(config['GENERAL']['check_interval'])

JOBS = [
    {
        'job_name': config['JOBS'][f'job{i}_name'],
        'file_name': config['JOBS'][f'job{i}_file_name'],
        'folder': config['JOBS'][f'job{i}_folder']
    }
    for i in range(1, (len(config['JOBS'].items()) // 3) + 1)  # Assuming each job has 3 items (name, filename, folder)
]

EMAIL_RECIPIENTS = [email.strip() for email in config['EMAIL']['recipients'].split(',')]


# Define the connection function
def smb_connect():
    try:
        conn = SMBConnection(
            USERNAME,
            PASSWORD,
            "client_machine",
            SERVER_NAME,
            domain=DOMAIN,
            use_ntlm_v2=True,
            is_direct_tcp=True
        )
        connected = conn.connect(SERVER_NAME, PORT)
        if connected:
            return conn
        else:
            #print(f"Failed to connect to {SERVER_NAME}")
            return None
    except Exception as e:
        print(f"Error: {e}")
        return None


def file_exists(conn, share_name, file_name):
    try:
        # List the files at the provided path
        files = conn.listPath(share_name, '/')
        
        # Check if the desired file is in the list
        for file in files:
            if file.filename == file_name:
                #print(f"File '{file_name}' exists on the network share.")
                return True
        #print(f"File '{file_name}' does not exist on the network share.")
        return False
    except Exception as e:
        #print(f"Error checking file existence: {e}")
        return False


def process_job(conn, job):
    if not file_exists(conn, SHARE_NAME, job['file_name']):
        #print(f"Checking for local files in folder {job['folder']} to upload...")
        local_files = sorted([f for f in os.listdir(job['folder']) if os.path.isfile(os.path.join(job['folder'], f))])
        if local_files:
            selected_file = local_files[0]
            #print(f"Found local file: {local_files[0]}")
            local_file_path = os.path.join(job['folder'], local_files[0])
            with open(local_file_path, 'rb') as f:
                #print(f"Uploading {local_files[0]} to {job['file_name']} on the network share...")
                conn.storeFile(SHARE_NAME, job['file_name'], f)
                #print(f"Successfully uploaded {local_files[0]} to {job['file_name']} on the network share.")

            # Create the archive folder if it doesn't exist
            archive_path = os.path.join(job['folder'], 'archive')
            if not os.path.exists(archive_path):
                os.makedirs(archive_path)

            # Move the local file to the archive folder
            shutil.move(local_file_path, os.path.join(archive_path, local_files[0]))
            #print(f"Moved local file: {local_files[0]} to the archive folder.")

            # Update remaining files count
            remaining_files = len(local_files) - 1

            # Send email
            subject = f"{job['job_name']} File: {selected_file} Uploaded to AS400"
            body = f"filename: {selected_file}\nhas been uploaded as: {job['file_name']}\nand is ready for processing.\n\nRemaining files in queue folder: {remaining_files}"
            recipients = EMAIL_RECIPIENTS
            send_email(subject, body, recipients)

        else:
            print(f"No local files in folder {job['folder']} found to upload.")


def send_email(subject, body, to_emails):
    # Email setup
    sender_email = os.getenv("SMTP_SENDER_EMAIL")
    sender_password = os.getenv("SMTP_SENDER_PASSWROD")

    # Set up the MIME
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = ", ".join(to_emails)
    message["Subject"] = subject
    message.attach(MIMEText(body, 'plain'))

    # Connect and send the email
    try:
        server = smtplib.SMTP(os.getenv("SMTP_SERVER"), 587)
        server.starttls()  # Encrypts the connection
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, to_emails, message.as_string())
        server.close()

        #print("Email sent successfully")
    except Exception as e:
        print(f"Error sending email: {e}")


# Main script logic

def main():
    while True:
        #print("Attempting to connect to SMB share...")
        conn = smb_connect()
        if conn:
            #print("Connected successfully to SMB share.")
            for job in JOBS:
                process_job(conn, job)
            conn.close()
        else:
            print("Failed to connect to SMB share.")

        #print(f"Sleeping for {CHECK_INTERVAL} seconds before next check...")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
