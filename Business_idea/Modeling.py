from bertopic import BERTopic
import pymongo
from Utility_functions import *
import gc

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.Airline_data
collection = db.removed_duplicates






#Batch_size to reduce memory usage
batch_size = 40000


# Initialize an empty list to collect all preprocessed tweets
preprocessed_tweets = []

# Process and collect all tweets in batches
for batch in batch_generator(collection, batch_size):
    preprocessed_tweets.extend(batch)
    gc.collect()

topic_model = BERTopic(nr_topics=25, min_topic_size= 100)

# Fit the model on the combined preprocessed tweets
topics, probs = topic_model.fit_transform(preprocessed_tweets)



print(topic_model.get_topic_info())

#Save model to a file
topic_model.save("bertopic_model")
