from Utility_functions import *
from bertopic import BERTopic
import json
from tqdm import tqdm


def make_topic_file(db):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client[db]
    collection = db.topic_analysis

    topic_model = BERTopic.load('merged_model')
    topic_labels = topic_model.custom_labels_
    query = {"id_str": 1, "topic": 1, "_id": 0}
    cursor = collection.find({}, {"id_str": 1, "topic": 1, "_id": 0})
    total_tweets = collection.count_documents(query)
    with tqdm(total=total_tweets, desc="Writing topics to file", unit="documents") as pbar:
        with open('topic_share.json', 'w') as file:
            for document in cursor:
                file.write(json.dumps({'id_str': document['id_str'], 'topic': new_labels(document['topic'], topic_labels)}) + '\n')
                pbar.update(1)






def get_topic(text, topic_model) -> str:
    
    topics, _ = topic_model.transform([text])
    return new_labels(topic_model.custom_labels[topics[0]], topic_model.custom_labels_)

def add_topics(db_name, collection_name):

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    collection = db[collection_name]
    total_tweets = collection.count_documents({})
    with open('topic_share.json') as file:
        with tqdm(total= total_tweets, desc="Writing topics to collection", unit="documents") as pbar:
            json_data = json.loads(file)
            for tweet in json_data:
                id_str = tweet['id_str']
                topic = tweet['topic']
        
                result = collection.update_one(
                {'id_str': id_str},
                {'$set': {'topic': topic}})

                if result.modified_count > 0:
                    pbar.update(1)
                else:
                    print(f"No document found with id_str: {id_str} or no update was needed.")
                    pbar.update(1)



def tweets_without_topic(db_name, collection_name):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    collection = db[collection_name]
    topic_model = BERTopic.load('merged_model')
    old_labels = topic_model.custom_labels_
    documents_without_topic = collection.find({'topic': {'$exists': False}})
    length = len(documents_without_topic)
    print(length)
    with tqdm(total= length, desc="Correcting tweets without topic", unit="documents") as pbar:
        for doc in documents_without_topic:
            topic = get_topic(get_full_text(doc), old_labels)
            id_str = doc['id_str']

            result = collection.update_one(
                {'id_str': id_str},
                {'$set': {'topic': topic}})
            
            if result.modified_count > 0:
                pbar.update(1)
            else:
                print(f"No document found with id_str: {id_str} or no update was needed.")
                pbar.update(1)
