from pymongo import MongoClient, InsertOne
from tqdm import tqdm

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client.AirplaneMode

# Collections
no_inconsistency_collection = db.needed_fields
no_single_tweets_collection = db.no_single_tweets

# MongoDB query to filter out tweets where in_reply_to_status_id_str is null and counted_reply is 0
query = {
    "$or": [
        {"in_reply_to_status_id_str": {"$ne": None}},
        {"counted_reply": {"$gt": 0}}
    ]
}

# Function to filter and insert tweets into the no_single_tweets collection
def filter_and_insert_tweets():
    # Fetch all documents matching the query from no_inconsistency_collection
    all_tweets = list(no_inconsistency_collection.find(query, {'_id': 0}))
    
    # Insert filtered tweets into no_single_tweets_collection
    if all_tweets:
        with tqdm(total=len(all_tweets), desc="Inserting tweets") as pbar:
            requests = [InsertOne(doc) for doc in all_tweets]
            no_single_tweets_collection.bulk_write(requests)
            pbar.update(len(all_tweets))
        return len(all_tweets)
    else:
        return 0

# Call the function
filtered_tweet_count = filter_and_insert_tweets()
print(f"{filtered_tweet_count} tweets filtered and inserted into no_single_tweets_collection.")

# Close the connection
client.close()
