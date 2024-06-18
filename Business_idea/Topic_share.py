# from Utility_functions import *
# from bertopic import BERTopic
# import json
# from tqdm import tqdm


# def make_topic_file(db):
#     client = pymongo.MongoClient("mongodb://localhost:27017/")
#     db = client[db]
#     collection = db.topic_analysis

#     topic_model = BERTopic.load('merged_model')
#     topic_labels = topic_model.custom_labels_
#     query = {"id_str": 1, "topic": 1, "_id": 0}
#     cursor = collection.find({}, {"id_str": 1, "topic": 1, "_id": 0})
#     total_tweets = collection.count_documents(query)
#     with tqdm(total=total_tweets, desc="Writing topics to file", unit="documents") as pbar:
#         with open('topic_share.json', 'w') as file:
#             for document in cursor:
#                 file.write(json.dumps({'id_str': document['id_str'], 'topic': new_labels(document['topic'], topic_labels)}) + '\n')
#                 pbar.update(1)






# def get_topic(text, topic_model) -> str:
    
#     topics, _ = topic_model.transform([text])
#     return new_labels(topic_model.custom_labels[topics[0]], topic_model.custom_labels_)

# def add_topics(db_name, collection_name):

#     client = pymongo.MongoClient("mongodb://localhost:27017/")
#     db = client[db_name]
#     collection = db[collection_name]
#     new_collection = db.topics


#     total_tweets = collection.count_documents({})


#     with open('topic_share.json', 'r') as file:
#         file_gen = file.readlines()
#         with tqdm(total= total_tweets, desc="Writing topics to collection", unit="documents") as pbar:

#             for doc in collection.find({}):
#                 for line in file_gen:

#                     data = json.loads(line)

#                     if str(data['id_str']) == str(doc['id_str']):
#                         doc['topic'] = data['topic']
#                         break

#                 new_collection.insert_one(doc)
#                 pbar.update(1)



# def tweets_without_topic(db_name):
#     client = pymongo.MongoClient("mongodb://localhost:27017/")
#     db = client[db_name]
#     collection = db['topics']
#     topic_model = BERTopic.load('merged_model')
#     old_labels = topic_model.custom_labels_
#     documents_without_topic = collection.find({'topic': {'$exists': False}})
#     with tqdm(desc="Correcting tweets without topic", unit="documents") as pbar:
#         for doc in documents_without_topic:
#             topic = get_topic(get_full_text(doc), old_labels)
#             id_str = doc['id_str']

#             result = collection.update_one(
#                 {'id_str': id_str},
#                 {'$set': {'topic': topic}})
            
#             if result.modified_count > 0:
#                 pbar.update(1)
#             else:
#                 print(f"No document found with id_str: {id_str} or no update was needed.")
#                 pbar.update(1)


import pymongo
from bertopic import BERTopic
import json
from tqdm import tqdm
import os

from Utility_functions import *


def make_topic_file(db_name):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    collection = db.topic_analysis

    # Load the BERTopic model
    topic_model = BERTopic.load('merged_model')
    topic_labels = topic_model.custom_labels_

    # Define the query
    query = {"id_str": 1, "topic": 1, "_id": 0}
    cursor = collection.find({}, {"id_str": 1, "topic": 1, "_id": 0})

    # Count the total documents
    total_tweets = collection.count_documents({})

    # Create and write to the topic_share.json file
    with tqdm(total=total_tweets, desc="Writing topics to file", unit="documents") as pbar:
        with open('topic_share.json', 'w') as file:
            for document in cursor:
                file.write(json.dumps({'id_str': document['id_str'], 'topic': new_labels(document['topic'], topic_labels)}) + '\n')
                pbar.update(1)


def get_topic(text, topic_model) -> str:
    topics, _ = topic_model.transform([text])
    return new_labels(topic_model.custom_labels[topics[0]], topic_model.custom_labels_)


def add_topics(db_name, collection_name):
    if not os.path.exists('topic_share.json'):
        raise FileNotFoundError("The file 'topic_share.json' does not exist. Please run make_topic_file first.")

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    collection = db[collection_name]
    new_collection = db.topics

    total_tweets = collection.count_documents({})

    with open('topic_share.json', 'r') as file:
        file_gen = file.readlines()
        with tqdm(total=total_tweets, desc="Writing topics to collection", unit="documents") as pbar:
            for doc in collection.find({}):
                for line in file_gen:

                    data = json.loads(line)
                    if str(data['id_str']) == str(doc['id_str']):
                        doc['topic'] = data['topic']
                        del file_gen[line.index()]
                        break
                new_collection.insert_one(doc)
                pbar.update(1)


def tweets_without_topic(db_name):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    collection = db['topics']
    topic_model = BERTopic.load('merged_model')
    old_labels = topic_model.custom_labels_
    documents_without_topic = collection.find({'topic': {'$exists': False}})
    with tqdm(desc="Correcting tweets without topic", unit="documents") as pbar:
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


# Example usage
if __name__ == "__main__":
    db_name = 'your_database_name'
    collection_name = 'your_collection_name'

    # Create topic file
    make_topic_file(db_name)

    # Add topics to documents
    add_topics(db_name, collection_name)

    # Correct tweets without topics
    tweets_without_topic(db_name)
