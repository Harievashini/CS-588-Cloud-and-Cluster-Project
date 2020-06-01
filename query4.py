# Importing pymongo package and datetime package
from pymongo import MongoClient
from datetime import datetime

# Passing MongoClient a host name and a port number.
client = MongoClient("mongodb://127.0.0.1:27017")
detector=[]  # List to store the detector ids
total_time=[]  # List to store the peak period travel times
# sum speeds of different detector ids
sum1=0 # sum the average speeds at 7 to 9 am
sum2=0 # sum the average speeds at 4 to 6 pm
with client:
	db = client.project # Opening Database project
	data = db.metadata_station.find({'locationtext':'Foster NB'})  # Retrieving the documents whose locationtext is Foster NB from collection metadata_station
	for station in data:
		length = station['length'] # Length of the station
		r=station['detectors'] # To fetch the detectors object
		for i in r:
			detector.append(i['detectorid'])
	for i in detector:
		aggregate_data1 = [{ '$match': {'$and': [ { 'detectorid':i }, { "starttime" : {"$gte":datetime.fromisoformat('2011-09-22T07:00:00.070+00:00'),"$lt":datetime.fromisoformat('2011-09-22T09:00:00.070+00:00')}}] }},{ '$group': {'_id': "null", 'Avgspeed': { '$avg': "$speed" } }}]
		avg_speed1 = list(db.loopdata.aggregate(aggregate_data1)) # Retrieving average speeds in 7 to 9 am time period from loopdata collection
		sum1+=avg_speed1[0]['Avgspeed']
		aggregate_data2 = [{ '$match': {'$and': [ { 'detectorid':i }, { "starttime" : {"$gte":datetime.fromisoformat('2011-09-22T16:00:00.070+00:00'),"$lt":datetime.fromisoformat('2011-09-22T18:00:00.070+00:00')}}] }},{ '$group': {'_id': "null", 'Avgspeed': { '$avg': "$speed" } }}]
		avg_speed2 = list(db.loopdata.aggregate(aggregate_data2)) # Retrieving average speeds in 4 to 6 pm time period from loopdata collection
		sum2+=avg_speed2[0]['Avgspeed']
	totalavgspeed=sum1/len(detector) # total average speed at 7 to 9 am
	timetaken=(length/totalavgspeed)*3600 # travel time in seconds for time period 7 to 9 am
	total_time.append(timetaken)
	totalavgspeed=sum2/len(detector) # total average speed at 4 to 6 pm
	timetaken=(length/totalavgspeed)*3600  # travel time in seconds for time period 4 to 6 pm
	total_time.append(timetaken)
	print("Peak Period Travel Times :")
	print("Total time in seconds - ",total_time)
