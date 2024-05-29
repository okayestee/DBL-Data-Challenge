from bertopic import BERTopic
import pymongo
from Text_cleaning import clean
import gc
import torch
from transformers import pipeline

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.Airline_data
collection = db.removed_duplicates

#Batch_size to reduce memory usage
batch_size = 5000

def batch_generator(collection, batch_size):
    total_tweets = collection.count_documents({})
    for i in range(0, total_tweets, batch_size):
        tweets_cursor = collection.find({}, {'_id': 0, 'tweet_text': 1}).skip(i).limit(batch_size)
        batch = list(tweets_cursor)
        yield [clean(tweet['tweet_text']) for tweet in batch]
        del batch
        gc.collect()


# Initialize BERTopic model
topic_model = BERTopic()

# Fit the model in batches
for batch in batch_generator(collection, batch_size):
    topic_model.partial_fit(batch)
    gc.collect()

#Save model to a file
topic_model.save("bertopic_model")
# Display the topics
print(topic_model.get_topic_info())