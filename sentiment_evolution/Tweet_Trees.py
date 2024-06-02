import pymongo
from pymongo import MongoClient, ASCENDING
from tqdm import tqdm
import threading
from queue import Queue
from treelib import Tree

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client.AirplaneMode
tweets_collection = db.no_single_tweets
tweet_trees_collection = db.Tweet_Trees

# Create an index to speed up the search
tweets_collection.create_index([("id_str", ASCENDING)])
tweets_collection.create_index([("in_reply_to_status_id_str", ASCENDING)])

# Function to build tree using treelib
def build_tree(tweet):
    tree = Tree()
    tree.create_node(tweet['id_str'], tweet['id_str'], data=tweet)
    
    def add_children(parent_id):
        children = tweets_collection.find({"in_reply_to_status_id_str": parent_id})
        for child in children:
            child_id = child['id_str']
            tree.create_node(child_id, child_id, parent=parent_id, data=child)
            add_children(child_id)
    
    add_children(tweet['id_str'])
    
    # Return the tree
    return tree

# Function to process a batch of starting tweets
def process_batch(batch):
    for tweet in batch:
        tree = build_tree(tweet)
        if len(tree.nodes) > 1:
            tree_dict = tree.to_dict(with_data=True)
            tweet_trees_collection.insert_one({"root_id": tweet['id_str'], "tree": tree_dict})

# Thread worker function
def worker():
    while True:
        batch = queue.get()
        if batch is None:
            queue.task_done()
            break
        process_batch(batch)
        queue.task_done()

# Initialize queue and threading
queue = Queue()
threads = []
num_threads = 4

for i in range(num_threads):
    thread = threading.Thread(target=worker)
    thread.start()
    threads.append(thread)

# Main loop to process tweets in batches
batch_size = 10000
total_tweets = tweets_collection.count_documents({"in_reply_to_status_id_str": None})

with tqdm(total=total_tweets, desc="Processing starting tweets") as pbar:
    for i in range(0, total_tweets, batch_size):
        batch = list(tweets_collection.find({"in_reply_to_status_id_str": None}).skip(i).limit(batch_size))
        queue.put(batch)
        pbar.update(len(batch))
    
    queue.join()

# Stop workers
for i in range(num_threads):
    queue.put(None)
for thread in threads:
    thread.join()

print("Processing complete.")
