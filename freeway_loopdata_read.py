from pymongo import MongoClient
from datetime import datetime
import csv

def read_loopdata(db,csvfile):
	with open(csvfile, 'r') as file:
		my_reader = csv.reader(file, delimiter=',')
		for row in my_reader:
			detectorid = int(row[0])
			starttime = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S-%f")
			if not row[2]:
				volume= None
			else:
				volume = int(row[2])
			if not row[3]:
				speed= None
			else:
				speed = int(row[3])
			if not row[4]:
				occupancy= None
			else:
				occupancy = int(row[4])
			if not row[5]:
				status= None
			else:
				status = int(row[5])
			if not row[6]:
				dqflags= None
			else:
				dqflags = int(row[6])

			document = {'detectorid':detectorid,'starttime': starttime,'volume':volume,'speed':speed,'occupancy':occupancy,'status':status,'dqflags':dqflags}
			result = db.loopdata.insert_one(document)
	
client = MongoClient("mongodb://127.0.0.1:27017")
db=client.project
read_loopdata(db,'freeway_loopdata.csv')

