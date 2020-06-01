# Importing pymongo package
from pymongo import MongoClient

# Passing MongoClient a host name and a port number.
client = MongoClient("mongodb://127.0.0.1:27017")
db = client.project # Opening Database project
data = db.loopdata.count_documents({'speed':{"$gt":100}}) # Counting the number of records that have high speed from loopdata collection
print("Count high speeds :",data)
	
