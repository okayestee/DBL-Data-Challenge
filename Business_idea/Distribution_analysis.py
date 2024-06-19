import pymongo
from bertopic import BERTopic
from Utility_functions import *
from tqdm import tqdm
from Distribution_vis import show_vis_topics
from datetime import datetime


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.Airline_data
collection = db.topic_analysis

def topic_distribution_airline(collection, tag) -> dict:
    
    pipeline = [
    {
        "$match": {
            "text": {"$regex": tag, "$options": "i"}  # "i" for case-insensitive matching
        }
    },
    {
        "$group": {
            "_id": "$topic",
            "count": {"$sum": 1}
        }
    }
    ]

    # Execute the aggregation
    results = collection.aggregate(pipeline)

    # Convert the results to a dictionary
    topic_counts = {result['_id']: result['count'] for result in results}

    return topic_counts

def topic_distribution(collection) -> dict:
    pipeline = [
    {
        "$group": {
            "_id": "$topic",
            "count": {"$sum": 1}
        }
    }
    ]

    # Execute the aggregation
    results = collection.aggregate(pipeline)

    # Convert the results to a dictionary
    topic_counts = {result['_id']: result['count'] for result in results}

    return topic_counts


def topic_distribution_timeframe(collection, start_date, end_date) -> dict:

    start_date = datetime(start_date[0],start_date[1],start_date[2])  # example: May 1, 2019
    end_date = datetime(end_date[0],end_date[1],end_date[2])    # example: June 1, 2019

    pipeline = [
    {
        "$group": {
            "_id": "$topic",
            "count": {"$sum": 1}
        },
         '$match': {
            'created_at': {
                '$gte': start_date,
                '$lt': end_date
            }
        }
    }
    ]

    # Execute the aggregation
    results = collection.aggregate(pipeline)

    # Convert the results to a dictionary
    topic_counts = {result['_id']: result['count'] for result in results}

    return topic_counts
