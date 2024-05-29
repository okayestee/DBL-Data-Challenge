from pymongo import MongoClient, ASCENDING
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import os

# Function to connect to the MongoDB server and select collections
def connect_to_db():
    # Connect to the MongoDB server
    client = MongoClient('mongodb://localhost:27017/')
    # Select the database
    db = client.DBL2
    # Select the collections
    no_inconsistency = db['no_inconsistency']
    return db, no_inconsistency

# Function to create indexes on relevant fields
def create_indexes(collection):
    # Create indexes to speed up the queries
    collection.create_index([("user.id_str", ASCENDING)])
    collection.create_index([("in_reply_to_status_id_str", ASCENDING)])



def find_replies_in_batches(collection, batch_size):
    # Query to match tweets that are replies
    query = {
        "in_reply_to_user_id_str": {"$ne": None}  # Update to filter out documents where in_reply_to_user_id_str is not null
    }
    
    # Fetch tweets in batches
    tweets = []
    total_docs = collection.count_documents(query)
    for skip in range(0, total_docs, batch_size):
        batch = list(collection.find(query).skip(skip).limit(batch_size))
        if not batch:
            break
        tweets.extend(batch)
    
    return tweets


# Function to store tweets in a new collection
def store_tweets_in_new_collection(db, tweets):
    # Create or get the 'replies' collection
    new_collection = db['replies']
    # Insert the tweets into the new collection
    if tweets:  # Check if there are any tweets to insert
        try:
            new_collection.insert_many(tweets)
            print(f"Inserted {len(tweets)} tweets into 'replies' collection.")
        except Exception as e:
            print(f"Error inserting documents: {e}")

def main():
    # Connect to the database and select the collection
    db, no_inconsistency_collection = connect_to_db()
    # Create indexes on relevant fields
    create_indexes(no_inconsistency_collection)
    # Find replies in batches
    replies = find_replies_in_batches(no_inconsistency_collection, batch_size=10000)
    # Store tweets in a new collection
    store_tweets_in_new_collection(db, replies)

   
