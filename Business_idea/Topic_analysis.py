import pymongo
from Utility_functions import *
from Sentiment_analysis import VADER_implementation
import BERTopic

topic_model = BERTopic.load("random_bertopic_model")

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.Airline_data
collection = db.removed_duplicates

batch_mean_compound: list[float] = []

if 'topic_analysis' not in db.list_collection_names():
    new_collection = db.create_collection("sentiment_analysis")
else:
    new_collection = db['topic_analysis']


batch_size = 10000
documents_processed = 0

trunc_error_counter = 0

while True:
    # Retrieve a batch of documents
    batch = list(collection.find({}).skip(documents_processed).limit(batch_size))
    if not batch:
        print('Topic Analysis Finished!')
        break  # Exit loop if no more documents are retrieved
    


    # Analyze sentiment for each document in the batch
    for document in batch:
        text = clean(get_full_text(document))


        # Transform the new document to identify its topic
        topics, probs = topic_model.transform(text)

        # Get the topic and its name
        topic_number = topics[0]
        topic = topic_model.get_topic(topic_number)



        VADER_implementation.add_entire_document(document, new_collection)


        new_collection.update_one()

        VADER_implementation.add_to_document(document['id'], 'Topic', topic, new_collection)

    # Update the count of processed documents
    documents_processed += len(batch)
    print(documents_processed)
