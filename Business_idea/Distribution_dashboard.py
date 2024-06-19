from Distribution_analysis import *
from Distribution_vis import *
from Utility_functions import *
from bertopic import BERTopic

#This is to get visualisations of the distibution of topics


# name you wnat to have displayed at the top of the barchart
Name = 'American airlines'

# database name and collection name
db = 'Airline_data'
collection: str = 'topics_real'
n_topics: int = 5
is_airline: bool = True
#only works when is_airline is set to True
tag: str = '@americanair'

# from here on do not touch the code
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client[db]
collection = db[collection]

if is_airline:
    show_vis_topics(topic_distribution_airline(collection, tag), Name, n_topics, is_airline)
else:
    show_vis_topics(topic_distribution(collection),Name, n_topics)

