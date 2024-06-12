from Utility_functions import *
from bertopic import BERTopic
import json


def make_topic_file(db):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client[db]
    collection = db.topic_analysis

    topic_model = BERTopic.load('merged_model')
    topic_labels = topic_model.custom_labels_

    cursor = collection.find({}, {"id_str": 1, "topic": 1, "_id": 0})


    with open('topic_share.json', 'w') as file:
        for document in cursor:
            file.write(json.dumps({'id_str': document['id_str'], 'topic': new_labels(document['topic'], topic_labels)}) + '\n')







def find_no_topic_tweets(db, collection):
    pass


def add_topics(db, collection):
    pass