import pymongo
import json
from pymongo import MongoClient, InsertOne
import os

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.DBL2
collection = db.mycollection

directory = '/Users/esteerutten/DataScienceFiles/DBL Data Challenge Map/data airlines'

#Iterate over each file in the directory

# Iterate over each file in the directory
for filename in os.listdir(directory):
    if filename.endswith(".json"):
        filepath = os.path.join(directory, filename)
        requesting = []
        with open(filepath) as f:
            for jsonObj in f:
                myDict = json.loads(jsonObj)
                requesting.append(InsertOne(myDict))
        
        # Perform bulk write operation for the current file
        result = collection.bulk_write(requesting)

client.close()