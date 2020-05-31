from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://127.0.0.1:27017")
route=[]
with client:
	db = client.project
	data = db.metadata_station.find({'locationtext':'Johnson Cr NB',"highway_name":"I-205"})
	for station in data:	
		route.append(station['locationtext'])
		downstream = station['downstream']
	d1 = db.metadata_station.find({'locationtext': 'Columbia to I-205 NB',"highway_name":"I-205"})
	for s in d1:
		sid = s['downstream']
	while(downstream != sid):
		d=db.metadata_station.find({'stationid':downstream,"highway_direction":"NORTH"})
		for station in d:
			downstream=station['downstream']
			route.append(station['locationtext'])
	print("Route from Johnson Cr NB to Columbia to I-205 NB - ",route)