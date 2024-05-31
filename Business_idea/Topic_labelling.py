from bertopic import BERTopic
import pymongo
from Utility_functions import *
import gc

topic_model = BERTopic.load('bertopic_model')

topic_labels = {
}

topic_model.set_topic_labels(topic_labels)

print(topic_model.get_topic_info())