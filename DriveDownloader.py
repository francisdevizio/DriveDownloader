from __future__ import print_function
import pickle
import os.path
import psycopg2
import io
import zipfile
from configparser import ConfigParser
from pathlib import Path
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

CONFIG_FILENAME = "database.ini"
CONFIG_SECTION_GDRIVE = "googledrive"
CONFIG_SECTION_POSTGRES = 'postgresql'
CONFIG_DRIVE_TEAMDRIVEID = 'teamdriveid'
CONFIG_DRIVE_DATAFOLDERID = 'datafolderid'
# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly',
          'https://www.googleapis.com/auth/drive.readonly']
# This search query enumerates the files in the specified folder
DRIVE_SEARCH_QUERY = "'{0}' in parents"

def main():
    global GoogleDriveConfig
    GoogleDriveConfig = readConfig(CONFIG_SECTION_GDRIVE, CONFIG_FILENAME)
    global PostGresConfig
    PostGresConfig = readConfig(CONFIG_SECTION_POSTGRES, CONFIG_FILENAME)

    service = setupDriveCredentials()
    files = getFilesFromGDrive(service)
    for file in files:
        unzip(file)
        parse(file)
        #insertInPostgres()
    

def readConfig(section, filename=CONFIG_FILENAME):
    parser = ConfigParser()
    parser.read(filename)
    sectionParams = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            sectionParams[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return sectionParams

def setupDriveCredentials():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    service = build('drive', 'v3', credentials=creds)
    return service

def getFilesFromGDrive(service):
    query = DRIVE_SEARCH_QUERY.format(GoogleDriveConfig[CONFIG_DRIVE_DATAFOLDERID])
    # Call the Drive v3 API
    results = service.files().list(
        includeTeamDriveItems=True, supportsTeamDrives=True,
        corpora="teamDrive",teamDriveId=GoogleDriveConfig[CONFIG_DRIVE_TEAMDRIVEID], 
        q=query, pageSize=2,
        fields="nextPageToken, files(id, name)").execute()
    items = results.get('files', [])
    fileList = []
    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            fileList.append(downloadFile(service, item['id'], item['name'].replace(":", "-")))
    return fileList

def downloadFile(service, fileId, fileName):
    filePath = os.path.join(os.getcwd(), "files", fileName)
    print(u'Downloading: {0} ({1})'.format(fileId, fileName))
    fh = None
    #try:
    request = service.files().get_media(fileId=fileId)
    fh = io.FileIO(filePath, 'w+b')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%" % int(status.progress() * 100))
    #except (Exception) as error:
    #    print(error)
    #finally:
    #    fh.close()
    return filePath

def unzip(file):
    fileName = os.path.basename(file) # Gets the file name
    fileName = os.path.splitext(fileName)[0] # Removes the file extension
    with zipfile.ZipFile(file, 'r') as zip_ref:
        zip_ref.extractall(os.path.join("files", fileName))
    

def parse(jsonFile):
    print("Parsing {0}".format(os.path.basename(jsonFile)))
    #TODO

def insertInPostgres():
    conn = None
    try:
        conn = psycopg2.connect(**PostGresConfig)
        print('')
        print('Connected to PostgreSQL database, version:')
        cur = conn.cursor()
        cur.execute('SELECT version()')
        db_version = cur.fetchone()
        print(db_version)
        cur.close()
        
        # TODO
        
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
    

if __name__ == '__main__':
    main()