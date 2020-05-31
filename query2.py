from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://127.0.0.1:27017")
detector=[]
volume_count=[]
sum_volume=0
with client:
	db = client.project
	data = db.metadata_station.find({'locationtext':'Foster NB'})
	for station in data:
		r=station['detectors']
		for i in r:
			detector.append(i['detectorid'])
	for i in detector:
		#data1=db.loopdata.find({'detectorid':i,"starttime" : {"$gte":datetime.fromisoformat('2011-09-21T00:00:00.070+00:00'),"$lt":datetime.fromisoformat('2011-09-22T00:00:00.070+00:00')}})
		aggregate_data = [{ '$match': {'$and': [ { 'detectorid':i }, { "starttime" : {"$gte":datetime.fromisoformat('2011-09-21T00:00:00.070+00:00'),"$lt":datetime.fromisoformat('2011-09-22T00:00:00.070+00:00')}}] }},{ '$group': {'_id': "null", 'sumofvolume': { '$sum': "$volume" } }}]
		vol = list(db.loopdata.aggregate(aggregate_data))
		volume_count.append(vol)
	for i in volume_count:
		sum_volume +=i[0]['sumofvolume']	
	print("Total volume = ",sum_volume)
		