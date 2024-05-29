from pymongo import MongoClient, IndexModel, ASCENDING
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client.AirplaneMode

# Collections
replies_collection = db.replies
convo_starters_collection = db.airline_convo_starters
trees_collection = db.airline_trees

# Ensure indexes are created
convo_starters_collection.create_indexes([IndexModel([('id_str', ASCENDING)])])
replies_collection.create_indexes([IndexModel([('in_reply_to_status_id_str', ASCENDING)])])

# Initialize a dictionary to hold the tweet relationships
tweet_dict = {}

# Batch size for processing
batch_size = 10000

# Process conversation starters in batches with progress bar
processed_count = 0
convo_starters_total = convo_starters_collection.count_documents({})
with tqdm(total=convo_starters_total, desc="Processing convo starters") as pbar:
    while processed_count < convo_starters_total:
        cursor = convo_starters_collection.find({}).skip(processed_count).limit(batch_size)
        batch = list(cursor)
        
        if not batch:
            break
        
        # Add tweets to the dictionary
        for tweet in batch:
            tweet_dict[tweet['id_str']] = {'tweet': tweet, 'children': []}
        
        processed_count += len(batch)
        pbar.update(len(batch))

# Fetch replies and add them to the tweet_dict with progress bar
total_replies = replies_collection.count_documents({})
with tqdm(total=total_replies, desc="Processing replies") as pbar:
    cursor = replies_collection.find({})
    for reply in cursor:
        tweet_dict[reply['id_str']] = {'tweet': reply, 'children': []}
        pbar.update(1)

# Initialize a dictionary to keep track of trees
trees = {}

# Helper function to add nodes to a tree
def add_node(tweet_id, tree):
    if tweet_id not in tweet_dict:
        return None
    
    tweet_info = tweet_dict[tweet_id]
    tweet = tweet_info['tweet']
    parent_id = tweet.get('in_reply_to_status_id_str')
    
    # If this tweet is a reply to another tweet
    if parent_id and parent_id in tweet_dict:
        parent_node = add_node(parent_id, tree)
        if parent_node:
            parent_node['children'].append(tweet_info)
        else:
            return None
    else:
        # This is a root tweet
        tree[tweet_id] = tweet_info

    return tweet_info

# Function to process tweets in parallel
def process_tweet_batch(batch):
    local_trees = {}
    for tweet_id in batch:
        add_node(tweet_id, local_trees)
    return local_trees

# Create trees for each root tweet in parallel
all_tweet_ids = list(tweet_dict.keys())
num_threads = 4
with tqdm(total=len(all_tweet_ids), desc="Building trees") as pbar:
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(0, len(all_tweet_ids), batch_size):
            batch = all_tweet_ids[i:i + batch_size]
            futures.append(executor.submit(process_tweet_batch, batch))
        
        for future in as_completed(futures):
            result = future.result()
            trees.update(result)
            pbar.update(batch_size)

# Prepare the new collection for storing trees
trees_collection.drop()  # Drop the collection if it exists to start fresh

# Store each tree in the new collection
with tqdm(total=len(trees), desc="Storing trees") as pbar:
    for root_id, tree in trees.items():
        trees_collection.insert_one({"root_id": root_id, "tree": tree})
        pbar.update(1)

# Close the connection
client.close()
