from bertopic import BERTopic
import pymongo
from Utility_functions import *
import gc

topic_model = BERTopic.load('merged_model')

topic_labels = topic_model.generate_topic_labels(nr_words=2, topic_prefix=False, word_length=None, separator=', ', aspect=None) 
topic_model.set_topic_labels(topic_labels)
topic_model.set_topic_labels({1: "Qantas", 3: "Ryanair", 5: "EasyJet", 7: "Lufthansa", 16: 'KLM', 4: 'Baggage', 18: 'Roundtrip', 27: 'Retweet'})

print(topic_model.get_topic_info())
topic_model.save('merged_model')