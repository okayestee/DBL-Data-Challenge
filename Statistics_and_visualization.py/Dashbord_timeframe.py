from timeframe_single_tweets import filter_tweets_by_date_manually
from filtering_start_and_end_date import filter_and_create_collection
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['DBL']  # Replace with your database name
start_date_str = "Tue Dec 31 00:00:00 +0000 2019"
end_date_str = "Wed Jan 01 00:00:00 +0000 2020"

filter_tweets_by_date_manually(db, 'Sentiment_included', 'Timeframe_filtered_tweets', start_date_str, end_date_str) #filtering individual tweets

# filter_and_create_collection(db, 'valid_trees_merged', 'timeframe_trees_merged', start_date_str, end_date_str) #filtering collections storing valid trees