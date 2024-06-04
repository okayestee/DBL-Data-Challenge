import pymongo
from bertopic import BERTopic
from Utility_functions import *
from tqdm import tqdm
from Distribution_vis import show_vis_topics


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.Airline_data
collection = db.topic_analysis

topic_model = BERTopic.load('merged_model')

user_info = [('@VirginAtlantic', 'Virgin Atlantic'),('@americanair', 'American Airlines'), ('@KLM', 'KLM'),('@ryanair', 'Ryanair'),('@airfrance','Air France'),('@British_Airways','British Airways'),('@easyJet','EasyJet'),('@lufthansa',"Lufthansa"),('@VirginAtlantic', 'Virgin Atlantic')]

def topic_distribution(docs, topic_labels:list) -> dict:
    topic_dict = dict()
    for document in docs:
        topic_label = get_real_label(document['topic'], topic_labels)
        if topic_label not in topic_dict:
            topic_dict[topic_label] = 1
        else:
            topic_dict[topic_label] += 1
    return topic_dict


for user in user_info:
    show_vis_topics(topic_distribution(filter_tweets_by_mention(collection, user[0]),topic_model.custom_labels_),user[1])
