# Importing pymongo package
from pymongo import MongoClient
import csv

# Retrieving the fields from freeway_detectors csv
def read_detectors(csvfile):
    detectors={} # detectors dictionary/ object to store the subfields
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

# Retrieving the fields from metadata_station csv
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

# Passing MongoClient a host name and a port number.   
client = MongoClient("mongodb://127.0.0.1:27017")
db=client.project # Opening Database project
stations = read_stations('metadata_station.csv') # Fetch documents from metadata_station csv
detectors = read_detectors('freeway_detectors.csv') # Fetch documents from freeway_detectors csv
for station in stations: 
    stationid = station['stationid'] 
    if stationid in detectors:
        station['detectors'] = detectors[stationid]    
    else:
        station['detectors'] = []
    result = db.metadata_station.insert_one(station) # Add the documents to metadata_station collection


