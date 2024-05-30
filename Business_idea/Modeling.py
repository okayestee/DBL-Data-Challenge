from bertopic import BERTopic
import pymongo
from Text_cleaning import clean
import gc
from Sentiment_analysis/V

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.Airline_data
collection = db.removed_duplicates

#Batch_size to reduce memory usage
batch_size = 30000

def batch_generator(collection, batch_size):
    tweets_cursor = collection.find({}, {'_id': 0, 'text': 1}).limit(batch_size)
    batch = list(tweets_cursor)
    yield [clean(tweet['text']) for tweet in batch]
    del batch
    gc.collect()




# Initialize an empty list to collect all preprocessed tweets
all_preprocessed_tweets = []


# Process and collect all tweets in batches
'''for batch in batch_generator(collection, batch_size):
    all_preprocessed_tweets.extend(batch)
    gc.collect()'''

# Initialize BERTopic with HDBSCAN clustering
topic_model = BERTopic(nr_topics=25, min_topic_size= 100)  # Adjust n_components as needed

# Fit the model on the combined preprocessed tweets
topics, probs = topic_model.fit_transform(batch_generator(collection, batch_size))

#Save model to a file
topic_model.save("bertopic_model")
# Display the topics
print(topic_model.get_topic_info())