from __future__ import print_function
from psycopg2 import extras
from configparser import ConfigParser
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from multiprocessing.pool import ThreadPool
import psycopg2
import pickle
import os.path
import io
import zipfile
import json

CONFIG_FILENAME = "database.ini"
CONFIG_SECTION_GDRIVE = "googledrive"
CONFIG_SECTION_POSTGRES = 'postgresql'
CONFIG_DRIVE_TEAMDRIVEID = 'teamdriveid'
CONFIG_DRIVE_DATAFOLDERID = 'datafolderid'
# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly',
          'https://www.googleapis.com/auth/drive.readonly']
# This search query enumerates the files in the specified folder {0}
# that were created after a date {1}
DRIVE_SEARCH_QUERY = "'{0}' in parents and createdTime > '{1}'"
DOWNLOAD_PAGE_SIZE = 5


def main():
    global GoogleDriveConfig
    GoogleDriveConfig = readConfig(CONFIG_SECTION_GDRIVE, CONFIG_FILENAME)
    global PostGresConfig
    PostGresConfig = readConfig(CONFIG_SECTION_POSTGRES, CONFIG_FILENAME)

    service = setupDriveCredentials()
    if not os.path.exists("files"):
        os.makedirs("files")
    
    # Build the GDrive query parameter
    dateNow = datetime.now() - timedelta(days=1)
    tzNow = dateNow.astimezone().isoformat(timespec='seconds')
    query = DRIVE_SEARCH_QUERY.format(GoogleDriveConfig[CONFIG_DRIVE_DATAFOLDERID], tzNow)

    filesLeft = True
    nextPageToken = None
    while (filesLeft):
        result = getFilesFromGDrive(query, service, nextPageToken)
        nextPageToken = result[0]
        print(nextPageToken)
        if (nextPageToken is None):
            filesLeft = False
        zips = result[1]
        for zip in zips:
            processZip(zip)

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
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    service = build('drive', 'v3', credentials=creds)
    return service

def getFilesFromGDrive(query, service, nextPageToken=None):
    # Call the Drive v3 API
    results = service.files().list(
        includeTeamDriveItems=True, supportsTeamDrives=True, pageToken=nextPageToken,
        corpora="teamDrive",teamDriveId=GoogleDriveConfig[CONFIG_DRIVE_TEAMDRIVEID], 
        q=query, pageSize=DOWNLOAD_PAGE_SIZE,
        fields="nextPageToken, files(id, name)").execute()
    pToken = results.get('nextPageToken', None)
    items = results.get('files', [])
    fileList = []
    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            fileList.append(downloadFile(service, item['id'], item['name'].replace(":", "-")))
    return (pToken, fileList)

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

def processZip(zip):
    folder = unzip(zip)
    filesList = os.listdir(folder)
    for file in filesList:
        data = parseJson(os.path.join(folder, file))
        if "tripupdates" in file:
            insertTripUpdatesInDB(data)
            print("Inserted " + file + " successfully in database.")
        elif "vehiclepositions" in file:
            insertVehiclePositionsInDB(data)
            print("Inserted " + file + " successfully in database.")

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

    # Get the last trip_update ID in the DB
    queryGetLastId = """
        SELECT MAX(trip_update_id) FROM public.trip_update
    """
    lastId = 0
    with psycopg2.connect(**PostGresConfig) as conn:
        with conn.cursor() as cur:
            cur.execute(queryGetLastId)
            lastId = cur.fetchone()[0]
    if lastId is None:
        lastId = 0
    
    # Insertion queries
    queryTripUpdate = """
        INSERT INTO public.trip_update
            (trip_update_id,
            trip_id,
            start_time,
            route_id,
            created_at)
        VALUES %s
    """
    queryStopTimeUpdate = """
        INSERT INTO public.stop_time_update
            (stop_id, 
            stop_sequence, 
            trip_update_id, 
            departure_time, 
            arrival_time, 
            schedule_relationship,
            created_at)
        VALUES %s
    """

    # JSON parsing
    paramsTripUpdate = []
    paramsStopUpdate = []
    for en in jsonData["entity"]:
        tripUp = en['tripUpdate']
        if tripUp["stopTimeUpdate"][0]['scheduleRelationship'] == 'NO_DATA':
            #print('Skipping trip ' + tripUp['trip']['tripId'] + " because no StopTimeUpdate data was found.")
            continue
        lastId += 1
        timestamp = None
        if 'timestamp' in tripUp:
            timestamp = datetime.utcfromtimestamp(int(tripUp['timestamp']))
        else:
            timestamp = None
        tripUpdate = (
            lastId,
            tripUp['trip']['tripId'],
            tripUp['trip']['startDate'] + ' ' + tripUp['trip']['startTime'],
            tripUp['trip']['routeId'],
            timestamp
        )
        paramsTripUpdate.append(tripUpdate)
        for stopTimeUpdate in tripUp["stopTimeUpdate"]:
            departureTime = None
            arrivalTime = None
            if 'departure' in stopTimeUpdate:
                departureTime = datetime.utcfromtimestamp(int(stopTimeUpdate['departure']['time']))
            else:
                departureTime = None
            if 'arrival' in stopTimeUpdate:
                arrivalTime = datetime.utcfromtimestamp(int(stopTimeUpdate['arrival']['time']))
            else:
                arrivalTime = None
            stopUpdate = (
                stopTimeUpdate['stopId'],
                stopTimeUpdate['stopSequence'],
                lastId,
                departureTime,
                arrivalTime,
                stopTimeUpdate['scheduleRelationship'],
                timestamp
                #TODO stop_time_id
            )
            paramsStopUpdate.append(stopUpdate)
            
    # Bulk insertion in database
    with psycopg2.connect(**PostGresConfig) as conn:
        print('')
        print('Connected to PostgreSQL database')
        with conn.cursor() as cur:
            #cur.execute(queryTripUpdate, paramsTripUpdate)
            #tripUpdateId = cur.fetchone()[0] # Fetch the ID that is returned by the DB
            #cur.execute(queryStopTimeUpdate, paramsStopUpdate)
            psycopg2.extras.execute_values(cur, queryTripUpdate, paramsTripUpdate, page_size=200)
            psycopg2.extras.execute_values(cur, queryStopTimeUpdate, paramsStopUpdate, page_size=200)

def insertVehiclePositionsInDB(jsonData):
    conn = None
    #queryVehicle = """
    #    INSERT INTO public.vehicle
    #        (vehicle_id)
    #    VALUES %s
    #    ON CONFLICT DO NOTHING
    #"""
    queryVehiclePositions = """
        INSERT INTO public.vehicle_position
            (vehicle_id, 
            trip_id, 
            current_stop_sequence, 
            current_status, 
            vehicle_lat, 
            vehicle_lon, 
            created_at)
        VALUES %s
    """
    with psycopg2.connect(**PostGresConfig) as conn:
        print('')
        print('Connected to PostgreSQL database')
        paramsVehicle = []
        data_list = []
        for en in jsonData["entity"]: 
            vehicle = en['vehicle']
            paramsVehicle.append(vehicle['vehicle']['id'])
            data = (
                vehicle['vehicle']['id'],
                vehicle['trip']['tripId'],
                vehicle['currentStopSequence'], 
                vehicle['currentStatus'], 
                vehicle['position']['latitude'], 
                vehicle['position']['longitude'], 
                datetime.utcfromtimestamp(int(vehicle['timestamp']))
            )
            data_list.append(data)
        with conn.cursor() as cur:
            #psycopg2.extras.execute_values(cur, queryVehicle, paramsVehicle, page_size=200)
            psycopg2.extras.execute_values(cur, queryVehiclePositions, data_list, page_size=200)
            

if __name__ == '__main__':
    main()
