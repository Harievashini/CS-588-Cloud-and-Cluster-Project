# Importing pymongo package and datetime package
from pymongo import MongoClient
from datetime import datetime

# Passing MongoClient a host name and a port number.
client = MongoClient("mongodb://127.0.0.1:27017")
route=[] # Route from Johnson Cr NB to Columbia
with client:
	db = client.project # Opening Database project
	data = db.metadata_station.find({'locationtext':'Johnson Cr NB',"highway_name":"I-205"}) # Retrieve document whose location text is johnson cr from metadata_station collection
	for station in data:	
		route.append(station['locationtext']) 
		downstream = station['downstream'] # The next station to Johnson cr
	d1 = db.metadata_station.find({'locationtext': 'Columbia to I-205 NB',"highway_name":"I-205"}) # Retrieve document whose location text is columbia from metadata_station collection
	for s in d1:
		sid = s['downstream'] # The next station to Columbia
	while(downstream != sid):
		d=db.metadata_station.find({'stationid':downstream,"highway_direction":"NORTH"}) # Retrieve document from metadata_station collection
		for station in d:
			downstream=station['downstream']
			route.append(station['locationtext'])
	print("Route Finding :")
	print("Route from Johnson Cr NB to Columbia to I-205 NB - ",route)
