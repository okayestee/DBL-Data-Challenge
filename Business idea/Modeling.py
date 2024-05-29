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
batch_size = 8000

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

# Use GPU for embedding with transformers
device = "cuda" if torch.cuda.is_available() else "cpu"
embedding_model = pipeline("feature-extraction", model="distilbert-base-uncased", device=0)

# Define a custom embedding function
def custom_embedding_function(texts):
    embeddings = embedding_model(texts)
    # Flatten the embeddings and convert to a format BERTopic can use
    embeddings = [embedding[0] for embedding in embeddings]
    return embeddings

# Create BERTopic model with custom embedding function
topic_model = BERTopic(embedding_model=custom_embedding_function)

# Fit the model on the preprocessed tweets
topics, probs = topic_model.fit_transform(all_tweets)

# Display the topics
print(topic_model.get_topic_info())
