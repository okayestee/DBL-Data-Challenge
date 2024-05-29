from pymongo import MongoClient, ASCENDING
from tqdm import tqdm

# Function to connect to the MongoDB server and select collections
def connect_to_db():
    # Connect to the MongoDB server
    client = MongoClient('mongodb://localhost:27017/')
    # Select the database
    db = client['AirplaneMode']
    # Select the collections
    removed_duplicates_collection = db['no_inconsistency']
    return db, removed_duplicates_collection

# Function to create indexes on relevant fields
def create_indexes(collection):
    # Create indexes to speed up the queries
    collection.create_index([("user.id_str", ASCENDING)])
    collection.create_index([("in_reply_to_status_id_str", ASCENDING)])

# Function to find replies in batches
def find_replies_in_batches(collection, batch_size):
    # Query to match tweets that are replies
    query = {
        "in_reply_to_status_id_str": {"$ne": None}
    }
    
    # Fetch tweets in batches
    tweets = []
    total_docs = collection.count_documents(query)
    
    # Setup progress bar
    with tqdm(total=total_docs, desc="Fetching replies", unit="tweet") as pbar:
        for skip in range(0, total_docs, batch_size):
            batch = list(collection.find(query).skip(skip).limit(batch_size))
            if not batch:
                break
            tweets.extend(batch)
            pbar.update(len(batch))
    
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
    db, removed_duplicates_collection = connect_to_db()
    # Create indexes on relevant fields
    create_indexes(removed_duplicates_collection)
    # Find replies in batches
    replies = find_replies_in_batches(removed_duplicates_collection, batch_size=10000)
    # Store tweets in a new collection
    store_tweets_in_new_collection(db, replies)

if __name__ == "__main__":
    main()
