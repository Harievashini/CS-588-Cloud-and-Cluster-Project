from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://127.0.0.1:27017")
detectors=[]
length=[]
time7to9 = []
time4to6=[]
with client:
	db = client.project
	data = db.metadata_station.find({"locationtext": {"$regex": 'NB$'},"highway_name":"I-205"})
	for station in data:
		length.append(station['length'])
		r=station['detectors']
		detector=[]
		for i in r:
			detector.append(i['detectorid'])
		detectors.append(detector)
	print(length)
	print(detectors)
	for i in detectors:
		sum1 = 0
		sum2 =0
		ct=0
		for j in i:
			#data1=db.loopdata.find({'detectorid':i,"starttime" : {"$gte":datetime.fromisoformat('2011-09-22T07:00:00.070+00:00'),"$lt":datetime.fromisoformat('2011-09-22T09:00:00.070+00:00')}})
			aggregate_data1 = [{ '$match': {'$and': [ { 'detectorid':j }, { "starttime" : {"$gte":datetime.fromisoformat('2011-09-22T07:00:00.070+00:00'),"$lt":datetime.fromisoformat('2011-09-22T09:00:00.070+00:00')}}] }},{ '$group': {'_id': "null", 'Avgspeed': { '$avg': "$speed" } }}]
			avg_speed1 = list(db.loopdata.aggregate(aggregate_data1))
			#print("1",avg_speed1[0]['Avgspeed'])
			if(avg_speed1[0]['Avgspeed']==None):
				avg_speed1[0]['Avgspeed']=0
			sum1+=avg_speed1[0]['Avgspeed']
			aggregate_data2 = [{ '$match': {'$and': [ { 'detectorid':j }, { "starttime" : {"$gte":datetime.fromisoformat('2011-09-22T16:00:00.070+00:00'),"$lt":datetime.fromisoformat('2011-09-22T18:00:00.070+00:00')}}] }},{ '$group': {'_id': "null", 'Avgspeed': { '$avg': "$speed" } }}]
			avg_speed2 = list(db.loopdata.aggregate(aggregate_data2))
			#print("2",avg_speed2[0]['Avgspeed'])
			if(avg_speed2[0]['Avgspeed']==None):
				avg_speed2[0]['Avgspeed']=0
			sum2+=avg_speed2[0]['Avgspeed']
		totalavgspeed=sum1/len(i)
		if(totalavgspeed == 0):
			timetaken = 0
		else:
			timetaken=(length[ct]/totalavgspeed)*60
		time7to9.append(timetaken)
		totalavgspeed=sum2/len(i)
		if(totalavgspeed == 0):
			timetaken = 0
		else:
			timetaken=(length[ct]/totalavgspeed)*60
		time4to6.append(timetaken)
		ct+=1
	print("Total list of times for 7to9 in mins - ",time7to9)
	print("Total list of times for 4to6 in mins -", time4to6)
	sum_7to9 =0
	sum_4to6 = 0
	for t in range(len(time7to9)):
		sum_7to9+=time7to9[t]
		sum_4to6+=time4to6[t]
	print("Total average time for 7to9 in mins - ",sum_7to9)
	print("Total average time for 4to6 in mins -", sum_4to6)

