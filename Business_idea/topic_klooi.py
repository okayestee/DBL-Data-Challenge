from bertopic import BERTopic
from Utility_functions import *
'''
topic_model = BERTopic.load('merged_model')
tweet = get_random_tweet(1)[0]

print(new_labels(tweet['topic'],topic_model.custom_labels_),'|',tweet['text'])'''
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['Airline_data']
collection = db['topics']
topic_model = BERTopic.load('merged_model')

documents_without_topic = collection.find({'topic': {'$exists': False}})
i = 0
for doc in documents_without_topic:
    i += 1
print(i)