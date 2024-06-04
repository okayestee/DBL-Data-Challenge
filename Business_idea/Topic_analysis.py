import pymongo
from Utility_functions import *
from bertopic import BERTopic
from tqdm import tqdm


topic_model = BERTopic.load("merged_model")

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.Airline_data
collection = db.no_inconsistency

batch_mean_compound: list[float] = []

if 'topic_analysis' not in db.list_collection_names():
    new_collection = db.create_collection("topic_analysis")
else:
    new_collection = db['topic_analysis']


batch_size = 100
documents_processed = 0

total_tweets = collection.count_documents({})
with tqdm(total=total_tweets, desc="Topic analysis", unit="documents") as pbar:
    for document in collection.find({}):
    

    
        text = clean(get_full_text(document))


        # Transform the new document to identify its topic
        topics, probs = topic_model.transform(text)

        # Get the topic and its name
        topic_number = topics[0]
        topic_name = topic_model.custom_labels_[topic_number]

        document['topic'] = topic_name

        new_collection.insert_one(document)

        # Update the count of processed documents
        pbar.update(1)


 

