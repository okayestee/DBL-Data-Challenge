import pymongo
import json
from pymongo import MongoClient, InsertOne
import os


#connect
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.Airline_data
collection = db.removed_duplicates

#pipeline 
pipeline = [
    {
            '$sample': { 'size': 1 }  # Get one random document
    },
    {
        "$project": {"text":1, '_id': 0
        }
    }
]
#aggregation
results = collection.aggregate(pipeline)

#print results
for result in results:
    print(result)