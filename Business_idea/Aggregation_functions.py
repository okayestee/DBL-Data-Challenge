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

def create_indexes(collection):
    # Create an index on the 'id_str' field & 'in_reply_to_status_id_str'
    #index_name_id = collection.create_index([('id_str', ASCENDING)])
    index_name_status = collection.create_index([('in_reply_to_status_id_str', ASCENDING)])
    return index_name_status


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
    return list(collection.aggregate(pipeline)

print(len(get_all_texts()))

def get_tweet_sample():