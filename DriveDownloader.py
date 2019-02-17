from __future__ import print_function
import pickle
import os.path
import io
import zipfile
import json
import psycopg2
from psycopg2 import extras
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


# To get the credentials.json file, go here https://developers.google.com/drive/api/v3/quickstart/python
# click the "Enable the Drive API" button and download the file.
def main():
    global GoogleDriveConfig
    GoogleDriveConfig = readConfig(CONFIG_SECTION_GDRIVE, CONFIG_FILENAME)
    global PostGresConfig
    PostGresConfig = readConfig(CONFIG_SECTION_POSTGRES, CONFIG_FILENAME)

    service = setupDriveCredentials()
    if not os.path.exists("files"):
        os.makedirs("files")
    files = getFilesFromGDrive(service)
    for file in files:
        folder = unzip(file)
        filesList = os.listdir(folder)
        for file in filesList:
            data = parseJson(os.path.join(folder, file))
            if "tripupdates" in file:
                insertTripUpdatesInDB(data)
                print("Inserted " + file + " successfully in database.")
            elif "vehiclepositions" in file:
                insertVehiclePositionsInDB(data)
                print("Inserted " + file + " successfully in database.")

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
    request = service.files().get_media(fileId=fileId)
    with io.FileIO(filePath, 'w+b') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print("Download %d%%" % int(status.progress() * 100))
    return filePath

def unzip(file):
    fileName = os.path.basename(file) # Gets the file name
    fileName = os.path.splitext(fileName)[0] # Removes the file extension
    with zipfile.ZipFile(file, 'r') as zip_ref:
        zip_ref.extractall(os.path.join("files", fileName))
    return os.path.join("files", fileName)

def parseJson(file):
    print("Parsing {0}".format(os.path.basename(file)))
    data = None
    with open(file) as j:
        data = json.load(j)
    return data

def insertTripUpdatesInDB(jsonData):
    conn = None
    queryTripUpdate = """
        INSERT INTO public.trip_update
            (trip_update_id,
            trip_id,
            start_date,
            route_id
            createdAt)
        VALUES (%s, %s, %s, to_timestamp(%s))
    """
    queryStopTimeUpdate = """
        INSERT INTO public.stop_time_update
            (stop_id, 
            stop_sequence, 
            trip_update_id, 
            departure_time, 
            arrival_time, 
            schedule_relationship, 
            stop_time_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    with psycopg2.connect(**PostGresConfig) as conn:
        print('')
        print('Connected to PostgreSQL database')
        for en in jsonData["entity"]:
            tripUp = en['tripUpdate']
            paramsTripUpdate = (
                tripUp['id'],
                tripUp['trip']['tripId'],
                tripUp['trip']['startTime'] + ' ' + tripUp['trip']['startDate'],
                tripUp['trip']['routeId'],
                tripUp['timestamp']
            )
            with conn.cursor() as cur:
                cur.execute(queryTripUpdate, paramsTripUpdate)
                for stopTimeUpdate in tripUp["stopTimeUpdate"]:
                    paramsStopUpdate = (
                        stopTimeUpdate['stopId'],
                        stopTimeUpdate['stopSequence'],
                        tripUp['id'],
                        stopTimeUpdate['departure']['time'],
                        stopTimeUpdate['arrival']['time'],
                        stopTimeUpdate['scheduleRelationship']
                        #TODO stop_time_id
                    )
                    cur.execute(queryStopTimeUpdate, paramsStopUpdate)

def insertVehiclePositionsInDB(jsonData):
    conn = None
    queryVehicle = """
        INSERT INTO public.vehicle
            (vehicle_id)
        VALUES (%s)
    """
    queryVehiclePositions = """
        INSERT INTO public.vehicle_position
            (vehicle_id, 
            trip_id, 
            current_stop_sequence, 
            current_status, 
            vehicle_lat, 
            vehicle_lon, 
            created_at)
        VALUES (%s, %s, %s, %s, %s, %s, to_timestamp(%s))
    """
    with psycopg2.connect(**PostGresConfig) as conn:
        print('')
        print('Connected to PostgreSQL database')
        #with conn.cursor() as cur:
        #    cur.execute('SELECT version()')
        #    db_version = cur.fetchone()
        #    print(db_version)

        #args = []
        for en in jsonData["entity"]:
            vehicle = en['vehicle']
            data = (
                vehicle['vehicle']['id'],
                vehicle['trip']['tripId'],
                vehicle['currentStopSequence'], 
                vehicle['currentStatus'], 
                vehicle['position']['latitude'], 
                vehicle['position']['longitude'], 
                vehicle['timestamp']
            )
            with conn.cursor() as cur:
                cur.execute(queryVehicle, (vehicle['vehicle']['id'],))
                cur.execute(queryVehiclePositions, data)
#            args.append(data)
#        print(args)
#        with conn.cursor() as cur:
#            args_str = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s)", x) for x in data)
#            cur.execute(queryVehiclePositions, args)
#            psycopg2.extras.execute_values(cur, queryVehiclePositions, args, template=None, page_size=100)
            

if __name__ == '__main__':
    main()
