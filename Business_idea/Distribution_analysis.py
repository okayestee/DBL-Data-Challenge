import pymongo
from bertopic import BERTopic
from Utility_functions import *
from tqdm import tqdm
from Distribution_vis import show_vis_topics


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.Airline_data
collection = db.topic_analysis

def topic_distribution(docs, topic_labels:list) -> dict:
    topic_dict = dict()
    for document in docs:
        topic_label = get_real_label(document['topic'], topic_labels)
        if topic_label not in topic_dict:
            topic_dict[topic_label] = 1
        else:
            topic_dict[topic_label] += 1
    return topic_dict

