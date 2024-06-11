from Distribution_analysis import *
from Distribution_vis import *
from Utility_functions import *
from bertopic import BERTopic

#This is to get visualisations of the distibution of topics


#user tag of the user you want to see the topic distribution of
user_tag = ''

# name you wnat to have displayed at the top of the barchart
Name = ''

# database name and collection name
db = ''
collection = ''





# from here on do not touch the code
topic_model = BERTopic.load('merged_model')

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client[db]
collection = db[collection]


show_vis_topics(topic_distribution(filter_tweets_by_mention(collection, user_tag),topic_model.custom_labels_),Name)
