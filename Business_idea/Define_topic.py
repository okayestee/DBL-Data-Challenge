from bertopic import BERTopic
import pymongo
from Utility_functions import *
import gc

topic_model = BERTopic.load('random_bertopic_model')


topics = topic_model.get_topic_info()
topics.to_excel('Topic_table.xlsx', index=False)

