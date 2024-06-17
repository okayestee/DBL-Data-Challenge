from Distribution_analysis import *
from Distribution_vis import *
from Utility_functions import *
from bertopic import BERTopic

#This is to get visualisations of the distibution of topics


# name you wnat to have displayed at the top of the barchart
Name = 'all data'

# database name and collection name
db = 'Airline_data'
collections = 'topics'
n_topics: int = 15





# from here on do not touch the code
topic_model = BERTopic.load('merged_model')

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client[db]
collection = db[collection]
show_vis_topics(topic_distribution(collection.find({})),Name, n_topics)

