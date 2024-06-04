from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['AirplaneMode']
collection = db['no_inconsistency'] #cleaned data
starting_tweets_collection = db['starting_tweets']

# Create an index on the in_reply_to_status_id field
collection.create_index([('in_reply_to_status_id', ASCENDING)])

# Batch size
batch_size = 10000

def fetch_and_store_batch(skip, limit):
    # Fetch batch of tweets with in_reply_to_status_id as None
    tweets = list(collection.find({'in_reply_to_status_id': None}).skip(skip).limit(limit))
    
    # Insert batch into the new collection
    if tweets:
        try:
            starting_tweets_collection.insert_many(tweets, ordered=False)
        except BulkWriteError as bwe:
            print(bwe.details)

# Get the total number of tweets that match the criteria
total_count = collection.count_documents({'in_reply_to_status_id': None})

# Progress bar setup
pbar = tqdm(total=total_count)

# Use ThreadPoolExecutor for parallel processing
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = []
    for skip in range(0, total_count, batch_size):
        futures.append(executor.submit(fetch_and_store_batch, skip, batch_size))
    
    for future in futures:
        # Update the progress bar for each completed future
        future.result()
        pbar.update(batch_size)

pbar.close()
client.close()
