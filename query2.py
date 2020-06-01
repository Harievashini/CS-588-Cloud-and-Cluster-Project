# Importing pymongo package and datetime package
from pymongo import MongoClient
from datetime import datetime

# Passing MongoClient a host name and a port number.
client = MongoClient("mongodb://127.0.0.1:27017")
detector=[] #List to store the detector ids
volume_count=[] # List to hold the volume counts
sum_volume=0 # Total volume
with client:
	db = client.project # Opening Database project
	data = db.metadata_station.find({'locationtext':'Foster NB'}) # Retrieving the documents whose locationtext is Foster NB
	for station in data:
		r=station['detectors'] # To fetch the detectors object
		for i in r:
			detector.append(i['detectorid'])
	for i in detector:
		aggregate_data = [{ '$match': {'$and': [ { 'detectorid':i }, { "starttime" : {"$gte":datetime.fromisoformat('2011-09-21T00:00:00.070+00:00'),"$lt":datetime.fromisoformat('2011-09-22T00:00:00.070+00:00')}}] }},
				  { '$group': {'_id': "null", 'sumofvolume': { '$sum': "$volume" } }}]
		vol = list(db.loopdata.aggregate(aggregate_data)) #volume count of the documents from loopdata collection
		volume_count.append(vol)
	for i in volume_count:
		sum_volume +=i[0]['sumofvolume'] #Total volume
	print("Total volume :",sum_volume)
		
		
