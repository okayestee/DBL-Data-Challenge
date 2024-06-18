import pymongo
from bertopic import BERTopic
from Utility_functions import *
from tqdm import tqdm
from Distribution_vis import show_vis_topics


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.Airline_data
collection = db.topic_analysis

def topic_distribution_test(collection,start_date: str = '2019-', end_date: str = '') -> dict:
    
    pipeline = [
    {
        "$group": {
            "_id": "$topic",
            "count": {"$sum": 1}
        },
        "$match":{
            'created_at':{
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


