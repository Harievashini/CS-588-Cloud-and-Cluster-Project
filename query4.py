from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://127.0.0.1:27017")
detector=[]
total_time=[]
sum1=0
sum2=0
with client:
	db = client.project
	data = db.metadata_station.find({'locationtext':'Foster NB'})
	for station in data:
		length = station['length']
		r=station['detectors']
		for i in r:
			detector.append(i['detectorid'])
	for i in detector:
		#data1=db.loopdata.find({'detectorid':i,"starttime" : {"$gte":datetime.fromisoformat('2011-09-22T07:00:00.070+00:00'),"$lt":datetime.fromisoformat('2011-09-22T09:00:00.070+00:00')}})
		aggregate_data1 = [{ '$match': {'$and': [ { 'detectorid':i }, { "starttime" : {"$gte":datetime.fromisoformat('2011-09-22T07:00:00.070+00:00'),"$lt":datetime.fromisoformat('2011-09-22T09:00:00.070+00:00')}}] }},{ '$group': {'_id': "null", 'Avgspeed': { '$avg': "$speed" } }}]
		avg_speed1 = list(db.loopdata.aggregate(aggregate_data1))
		sum1+=avg_speed1[0]['Avgspeed']
		aggregate_data2 = [{ '$match': {'$and': [ { 'detectorid':i }, { "starttime" : {"$gte":datetime.fromisoformat('2011-09-22T16:00:00.070+00:00'),"$lt":datetime.fromisoformat('2011-09-22T18:00:00.070+00:00')}}] }},{ '$group': {'_id': "null", 'Avgspeed': { '$avg': "$speed" } }}]
		avg_speed2 = list(db.loopdata.aggregate(aggregate_data2))
		sum2+=avg_speed2[0]['Avgspeed']
	totalavgspeed=sum1/len(detector)
	timetaken=(length/totalavgspeed)*3600
	total_time.append(timetaken)
	totalavgspeed=sum2/len(detector)
	timetaken=(length/totalavgspeed)*3600
	total_time.append(timetaken)
	print("Total time in seconds - ",total_time)