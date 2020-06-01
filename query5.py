# Importing pymongo package and datetime package
from pymongo import MongoClient
from datetime import datetime

# Passing MongoClient a host name and a port number.
client = MongoClient("mongodb://127.0.0.1:27017")
detectors=[] # List to store the detector ids
length=[] # List to store the length of all stations
time7to9 = [] # List to store the peak period travel times of all stations at 7 to 9 am
time4to6=[] # List to store the peak period travel times of all stations at 4 to 6 pm
with client:
	db = client.project # Opening Database project
	data = db.metadata_station.find({"locationtext": {"$regex": 'NB$'},"highway_name":"I-205"}) # Fetch all stations that are highway I-205 in north direction from collection metadata_station
	for station in data:
		length.append(station['length']) 
		r=station['detectors'] # To fetch the detectors object
		detector=[]
		for i in r:
			detector.append(i['detectorid'])
		detectors.append(detector)
	ct=0 # Index to retrive length of each station
	for i in detectors:
		sum1 = 0 # sum the average speeds of detectors at 7 to 9 am
		sum2 = 0 # sum the average speeds of detectors at 4 to 6 pm
		for j in i:
			aggregate_data1 = [{ '$match': {'$and': [ { 'detectorid':j }, { "starttime" : {"$gte":datetime.fromisoformat('2011-09-22T07:00:00.070+00:00'),"$lt":datetime.fromisoformat('2011-09-22T09:00:00.070+00:00')}}] }},
					   { '$group': {'_id': "null", 'Avgspeed': { '$avg': "$speed" } }}]
			avg_speed1 = list(db.loopdata.aggregate(aggregate_data1)) # Retrieving average speeds in 7 to 9 am time period from loopdata collection
			if(avg_speed1[0]['Avgspeed']==None):
				avg_speed1[0]['Avgspeed']=0
			sum1+=avg_speed1[0]['Avgspeed']
			aggregate_data2 = [{ '$match': {'$and': [ { 'detectorid':j }, { "starttime" : {"$gte":datetime.fromisoformat('2011-09-22T16:00:00.070+00:00'),"$lt":datetime.fromisoformat('2011-09-22T18:00:00.070+00:00')}}] }},
					   { '$group': {'_id': "null", 'Avgspeed': { '$avg': "$speed" } }}]
			avg_speed2 = list(db.loopdata.aggregate(aggregate_data2)) # Retrieving average speeds in 4 to 6 pm time period from loopdata collection 
			if(avg_speed2[0]['Avgspeed']==None):
				avg_speed2[0]['Avgspeed']=0
			sum2+=avg_speed2[0]['Avgspeed']
		totalavgspeed=sum1/len(i) # total average speed at 7 to 9 am
		# travel time in minutes for time period 7 to 9 am for each stations
		if(totalavgspeed == 0):
			timetaken = 0
		else:
			timetaken=(length[ct]/totalavgspeed)*60
		time7to9.append(timetaken)
		totalavgspeed=sum2/len(i) # total average speed at 4 to 6 pm
		# travel time in seconds for time period 4 to 6 pm
		if(totalavgspeed == 0):
			timetaken = 0
		else:
			timetaken=(length[ct]/totalavgspeed)*60
		time4to6.append(timetaken)
		ct=ct+1
	sum_7to9 =0 # sum of travel time in minutes for time period 7 to 9 am of all stations
	sum_4to6 = 0 # sum of travel time in minutes for time period 4 to 6 pm of all stations
	for t in range(len(time7to9)):
		sum_7to9+=time7to9[t]
		sum_4to6+=time4to6[t]
	print("Peak Period Travel Times :")
	print("Total time for 7 to 9 am in minutes - ",sum_7to9)
	print("Total time for 4 to 6 pm in minutes - ", sum_4to6)



