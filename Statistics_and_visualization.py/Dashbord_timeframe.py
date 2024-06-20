from timeframe_single_tweets import filter_tweets_by_date_manually
from filtering_start_and_end_date import filter_and_create_collection
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['DBL']  # Replace with your database name
start_date_str = "Sat Jun 01 00:00:00 +0000 2019"
end_date_str = "Mon Jul 01 00:00:00 +0000 2019"

# filter_tweets_by_date_manually(db, 'Sentiment_included', 'Timeframe_filtered_tweets', start_date_str, end_date_str) #filtering individual tweets
# filter_tweets_by_date_manually(db, 'AmericanAir_tweets', 'Timeframe_filtered_AA', start_date_str, end_date_str) #filtering individual tweets
# filter_and_create_collection(db, 'valid_trees_merged', 'timeframe_trees_merged', start_date_str, end_date_str) #filtering collections storing valid trees
filter_and_create_collection(db, 'valid_trees_user', 'timeframe_trees_user', start_date_str, end_date_str) #filtering collections storing valid trees
filter_and_create_collection(db, 'valid_trees_airline', 'timeframe_trees_airline', start_date_str, end_date_str) #filtering collections storing valid trees