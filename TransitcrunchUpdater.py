from __future__ import print_function
from psycopg2 import extras
from datetime import datetime
from datetime import timedelta
from threading import Thread
from configparser import ConfigParser
from google.protobuf import json_format
from google.transit import gtfs_realtime_pb2
import psycopg2
import zipfile
import os.path
import json
import urllib.request

BASE_STM_URL = 'https://api.stm.info/pub/od/gtfs-rt/ic/v1'
BASE_RTM_URL = 'http://opendata.amt.qc.ca:2539/ServiceGTFSR'
STM_GTFS_TRIP_UPDATE_URL = '%s/tripUpdates' % (BASE_STM_URL)
STM_GTFS_VEHICLE_POSITION_URL = '%s/vehiclePositions' % (BASE_STM_URL)

CONFIG_FILENAME = "database.ini"
CONFIG_SECTION_POSTGRES = 'postgresql'
CONFIG_SECTION_APIS = 'apikeys'
CONFIG_STM_API_KEY = 'stmapikey'
#CONFIG_RTM_API_KEY = 'rtmapikey'

class Downloader(Thread):
    def __init__(self, endpoint, apikey, dir_path, filename):
        Thread.__init__(self)
        self.endpoint = endpoint
        self.apikey = apikey
        self.filename = filename
        self.path = dir_path

    def run(self):
        data = ()
        headers = {'apikey': self.apikey}
        req = urllib.request.Request(self.endpoint, data, headers)
        api_response = urllib.request.urlopen(req)
        response_feed = gtfs_realtime_pb2.FeedMessage()
        response_feed.ParseFromString(api_response.read())
        json_string = json_format.MessageToJson(response_feed)
        response_timestamp = response_feed.header.timestamp
        file_name = self.filename+"_"+str(response_timestamp)+".json"
        file = open( os.path.join(self.path, file_name), "w+")
        file.write(json_string)
        file.close()

class RTM_downloader(Thread):
    def __init__(self, endpoint, dir_path, filename):
        Thread.__init__(self)
        self.endpoint = endpoint
        self.filename = filename
        self.path = dir_path
    
    def run(self):
        req = urllib.request.Request(self.endpoint)
        api_response = urllib.request.urlopen(req)
        response_feed = gtfs_realtime_pb2.FeedMessage()
        response_feed.ParseFromString(api_response.read())
        json_string = json_format.MessageToJson(response_feed)
        response_timestamp = response_feed.header.timestamp
        file_name = self.filename+"_"+str(response_timestamp)+".json"

        file = open(os.path.join(self.path, file_name), "w+")
        file.write(json_string)
        file.close()

def main():
    global StmApiConfig
    StmApiConfig = readConfig(CONFIG_SECTION_APIS, CONFIG_FILENAME)
    global PostGresConfig
    PostGresConfig = readConfig(CONFIG_SECTION_POSTGRES, CONFIG_FILENAME)

    STM_GTFS_API_KEY = StmApiConfig[CONFIG_STM_API_KEY]
    #RTM_TOKEN = StmApiConfig[CONFIG_RTM_API_KEY]
    #RTM_GTFS_TRIP_UPDATE_URL = '%s/TripUpdate.pb?token=%s' % (BASE_RTM_URL, RTM_TOKEN)
    #RTM_GTFS_VEHICLE_POSITION_URL = '%s/VehiclePosition.pb?token=%s' % (BASE_RTM_URL, RTM_TOKEN)

    path='downloads'
    #RTM_path='RTM_downloads'
    
    ### STM
    if not os.path.exists(path):
        os.makedirs(path)
    downloader_1 = Downloader(STM_GTFS_TRIP_UPDATE_URL, STM_GTFS_API_KEY, 'downloads', 'tripupdates')
    downloader_2 = Downloader(STM_GTFS_VEHICLE_POSITION_URL, STM_GTFS_API_KEY, 'downloads', 'vehiclepositions')
    #downloader_3 = RTM_downloader(RTM_GTFS_TRIP_UPDATE_URL, 'RTM_downloads', 'tripupdates')
    #downloader_4 = RTM_downloader(RTM_GTFS_VEHICLE_POSITION_URL, 'RTM_downloads', 'vehiclepositions')

    print('STM downloads started')
    downloader_1.start()
    downloader_1.join()
    downloader_2.start()
    downloader_2.join()
    print('STM downloads completed')
    
    ### RTM
    #if not os.path.exists(RTM_path):
    #    os.makedirs(RTM_path)
    #print('RTM downloads started')
    #downloader_3.start()
    #downloader_3.join()
    #downloader_4.start()
    #downloader_4.join()
    #print('RTM downloads completed')
    
    processFiles(path)

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

def processFiles(path):
    filesList = os.listdir(path)
    for file in filesList:
        filePath = os.path.join(path, file)
        data = parseJson(filePath)
        if "tripupdates" in file:
            success = insertTripUpdatesInDB(data)
            if (success): print("Inserted " + file + " successfully in database.")
        elif "vehiclepositions" in file:
            success = insertVehiclePositionsInDB(data)
            if (success): print("Inserted " + file + " successfully in database.")
        os.remove(filePath)
    print('')

def parseJson(file):
    print("Parsing {0}".format(os.path.basename(file)))
    data = None
    with open(file) as j:
        data = json.load(j)
    return data

def insertTripUpdatesInDB(jsonData):
    if "entity" not in jsonData:
        return False
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
        
        with conn.cursor() as cur:
            #cur.execute(queryTripUpdate, paramsTripUpdate)
            #tripUpdateId = cur.fetchone()[0] # Fetch the ID that is returned by the DB
            #cur.execute(queryStopTimeUpdate, paramsStopUpdate)
            print('Inserting TripUpdate...')
            psycopg2.extras.execute_values(cur, queryTripUpdate, paramsTripUpdate, page_size=200)
            print('Inserting StopTimeUpdate...')
            psycopg2.extras.execute_values(cur, queryStopTimeUpdate, paramsStopUpdate, page_size=200)
            return True
    return False

def insertVehiclePositionsInDB(jsonData):
    if "entity" not in jsonData:
        return False
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
            print('Inserting VehiclePositions...')
            psycopg2.extras.execute_values(cur, queryVehiclePositions, data_list, page_size=200)
            return True
    return False

if __name__ == '__main__':
    main()
