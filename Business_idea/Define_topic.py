from bertopic import BERTopic
import pymongo
from Utility_functions import *
import gc

topic_model = BERTopic.load('bertopic_model')

tweet = get_random_tweet()

print(topic_model.get_topic(get_full_text(tweet), True))
