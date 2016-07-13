from __future__ import print_function
import httplib2
import os
import io

from googleapiclient import errors
from apiclient import discovery
from apiclient.http import MediaIoBaseDownload
import oauth2client
from oauth2client import client
from oauth2client import tools
import argparse

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/drive-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/drive.readonly'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Drive API Python Quickstart'
FOLDER_ID = '0Bx8VoNOSwBLcVzMyUUxRaFpXZTA'

def get_credentials(flags):
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    script_dir = os.path.dirname(os.path.realpath(__file__))
    credential_dir = os.path.join(script_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'drive-python-quickstart.json')
    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def get_file_list(folder_id,service):
    results = service.children().list(
        folderId=folder_id).execute()
    items = results.get('items', [])
    return items

def clean_file_list(service,file_list):
    cleaned_file_list = []
    for x in file_list:
        f = service.files().get(fileId=x['id']).execute()
        if f['title'][-10:] == '_codes.csv':
            cleaned_file_list.append(f)
    return cleaned_file_list

def download_file(service,file_data,download_path):
    try:
        request = service.files().export_media(fileId=file_data['id'],
                                               mimeType='text/csv')
        fh = open(os.path.join(download_path,file_data['title']),'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print("Download {0} {1}%".format(file_data['title'],int(status.progress() * 100)))
        return True
    except errors.HttpError as e:
        print('skipped {0}: {1}'.format(file_data['title'],e))
        return False
        

def main():
    """Shows basic usage of the Google Drive API.

    Creates a Google Drive API service object and outputs the names and IDs
    for up to 10 files.
    """
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument('-o','--output_dir')
    parser.add_argument('-i','--drive_folder_id')
    flags = parser.parse_args()
    
    credentials = get_credentials(flags)
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v2', http=http)
    file_names = get_file_list(FOLDER_ID,service)
    if not file_names:
        print('No files found.')
    else:
        file_data = clean_file_list(service,file_names)
        for f in file_data:
            download_file(service,f,flags.output_dir)

if __name__ == '__main__':
    main()
