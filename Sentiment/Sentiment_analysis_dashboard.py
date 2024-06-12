from Sentiment import VADER_implementation as vader_imp
from Sentiment import Sentiment_stats as senti_stats
import pymongo

# Connect to the database
client = pymongo.MongoClient("mongodb://localhost:27017/") # Connect to MongoDB
db = client['DBL_data'] # Use the DBL database
collection = db['no_inconsistency'] # Choose a collection of tweets

# Create a new collection that includes all the same tweets but with the sentiment variables included
vader_imp.add_sentiment_variables(db, collection, 'Sentiment_included')

# Get the statistics and print them to the terminal
senti_stats.get_sentiment_stats(collection)
