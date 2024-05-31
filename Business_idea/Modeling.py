from bertopic import BERTopic
import pymongo
from Utility_functions import *
import gc

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.Airline_data
collection = db.removed_duplicates






#Batch_size to reduce memory usage
batch_size = 100000


# Initialize an empty list to collect all preprocessed tweets
preprocessed_tweets = [clean(get_full_text(tweet)) for tweet in get_random_tweet(batch_size)] 
gc.collect()
client.close()
# Process and collect all tweets in batches

topic_model = BERTopic(min_topic_size= 200)

# Fit the model on the combined preprocessed tweets
topics, probs = topic_model.fit_transform(preprocessed_tweets)



print(topic_model.get_topic_info())

#Save model to a file
topic_model.save("bertopic_model")
