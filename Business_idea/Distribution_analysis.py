import pymongo
from bertopic import BERTopic
from Utility_functions import *
from tqdm import tqdm
from Distribution_vis import show_vis_topics


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.Airline_data
collection = db.topic_analysis

def topic_distribution(docs) -> dict:
    topic_dict = dict()
    for document in docs:
        topic_label = document['topic']
        if topic_label not in topic_dict:
            topic_dict[topic_label] = 1
        else:
            topic_dict[topic_label] += 1
    return topic_dict

def topic_distribution_test(collection) -> dict:
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


