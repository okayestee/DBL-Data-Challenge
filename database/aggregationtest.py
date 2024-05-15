import pymongo
import json
from pymongo import MongoClient, InsertOne
import os


#connect
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.DBL2
collection = db.clean_finalversion

#pipeline 
pipeline = [
    {
        "$match": {
            "in_reply_to_screen_name": "AmericanAir",
            "text": {"$regex": "fuck", "$options": "i"}
        }
    }
]
#aggregation
results = collection.aggregate(pipeline)

#print results
for result in results:
    print(result)
