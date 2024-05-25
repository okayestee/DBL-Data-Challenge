import pymongo
import json
from pymongo import MongoClient, InsertOne
import os

def get_randomtweet_text() -> str:
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

    return list(collection.aggregate(pipeline))[0]['text']


def get_all_texts() -> list:

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client.Airline_data
    collection = db.removed_duplicates

    #pipeline 
    pipeline = [
        {
            "$project": {"text":1, '_id': 0
            }
        }
    ]
    return list(collection.aggregate(pipeline, allowDiskUse=True))

print(len(get_all_texts()))