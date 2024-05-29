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
batch_size = 4000

def process_batch(batch):
    tweets = [tweet['text'] for tweet in batch]
    tweets = [clean(tweet) for tweet in tweets]
    return tweets


#already processed tweets
all_tweets = []

total_tweets = collection.count_documents({})
for i in range(0,total_tweets, batch_size):
    tweets_cursor = collection.find({},{'_id': 0, 'text':1}).skip(i).limit(batch_size)
    batch = list(tweets_cursor)
    processed_batch = process_batch(batch)
    all_tweets.extend(processed_batch)
    #to free up memory
    del batch
    del processed_batch
    gc.collect()

#Topic Modeling with BERTopic
topic_model = BERTopic()
topics, probs = topic_model.fit_transform(all_tweets)

# Display the topics
print(topic_model.get_topic_info())
