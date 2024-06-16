from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

def user_convo_starters():
    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['DBL']
    starting_tweets_collection = db['starting_tweets']
    user_convo_starters_collection = db['user_convo_starters']

    # List of airline user ids
    airline_ids = [
        '56377143', '106062176', '18332190', '22536055', '124476322', '26223583', 
        '2182373406', '38676903', '1542862735', '253340062', '218730857', 
        '45621423', '20626359'
    ]

    # Create an index on the user.id_str field
    starting_tweets_collection.create_index([('user.id_str', ASCENDING)])

    # Batch size
    batch_size = 10000

    def fetch_and_store_batch(skip, limit):
        # Fetch batch of tweets where user.id_str does not match airline_ids
        tweets = list(starting_tweets_collection.find({'user.id_str': {'$nin': airline_ids}}).skip(skip).limit(limit))
        
        # Insert batch into the new collection
        if tweets:
            try:
                user_convo_starters_collection.insert_many(tweets, ordered=False)
            except BulkWriteError as bwe:
                print(bwe.details)

    # Get the total number of tweets that match the criteria
    total_count = starting_tweets_collection.count_documents({'user.id_str': {'$nin': airline_ids}})

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

# Call the function to execute the code
user_convo_starters()
