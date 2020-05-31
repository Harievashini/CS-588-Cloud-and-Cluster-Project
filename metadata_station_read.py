from pymongo import MongoClient
import csv

def read_detectors(csvfile):
    detectors={}
    with open(csvfile, 'r', encoding='utf-8-sig') as file:
        my_reader = csv.reader(file, delimiter=',')
        for row in my_reader:
            detectorid = int(row[0])
            lanenumber = int(row[1])
            stationid = int(row[2])
            
            detectordocument = {'detectorid': detectorid,'lanenumber': lanenumber}
            if stationid not in detectors:
                detectors[stationid] = []
            detectors[stationid].append(detectordocument)
    return detectors

def read_stations(csvfile):
    stations=[]
    with open(csvfile, 'r',encoding='utf-8-sig') as file:
        my_reader = csv.reader(file, delimiter=',')
        for row in my_reader:
            stationid = int(row[0])
            milepost = float(row[1])
            locationtext = row[2]
            upstream = int(row[3])
            downstream = int(row[4])
            numberlanes = int(row[5])
            latlon = row[6].replace(" ", ",", 1)
            length = float(row[7])
            highway_shortdirection = row[8]
            highway_direction = row[9]
            highway_name = row[10]

            station_document = {'stationid':stationid,'milepost':milepost,'locationtext':locationtext,'upstream':upstream,'downstream':downstream,'numberlanes':numberlanes,
                'latlon':latlon,'length':length,'highway_shortdirection':highway_shortdirection,'highway_direction':highway_direction,'highway_name':highway_name}
            stations.append(station_document)
    return stations

    
client = MongoClient("mongodb://127.0.0.1:27017")
db=client.project
stations = read_stations('metadata_station.csv')
detectors = read_detectors('freeway_detectors.csv')
for station in stations: 
    stationid = station['stationid']
    if stationid in detectors:
        station['detectors'] = detectors[stationid]    
    else:
        station['detectors'] = []
    result = db.metadata_station.insert_one(station)


