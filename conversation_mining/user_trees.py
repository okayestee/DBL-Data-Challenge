import pymongo
from pymongo import MongoClient, ASCENDING
from tqdm import tqdm
import threading
from queue import Queue

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client.AirplaneMode
user_convo_starters = db.user_convo_starters
replies = db.replies
user_trees = db.user_trees

# Create indexes to speed up the search
user_convo_starters.create_index([("id_str", ASCENDING)])
replies.create_index([("in_reply_to_status_id_str", ASCENDING)])

# Clear the user_trees collection before starting
user_trees.delete_many({})

# Function to build tree
def build_tree(tweet):
    # Initialize tree node with data
    tree = {"id_str": tweet['id_str'], "data": tweet, "children": []}
    
    # Remove the _id field from the tweet object
    tweet.pop('_id', None)
    
    # Recursive function to add children nodes
    def add_children(node):
        # Find replies for the current node
        children = replies.find({"in_reply_to_status_id_str": node["id_str"]})
        for child in children:
            # Remove the _id field from the child object
            child.pop('_id', None)
            # Create child node with its children recursively
            child_node = {"id_str": child['id_str'], "data": child, "children": []}
            child_node["children"] = add_children(child_node)  # Recursively add children
            # Append child node to current node's children list
            node["children"].append(child_node)
        return node["children"]
    
    # Add children to the root node
    tree["children"] = add_children(tree)
    
    # Return the constructed tree
    return tree

# Function to process a batch of tweets
def process_batch(batch, pbar):
    trees = []
    for tweet in batch:
        tree = build_tree(tweet)
        if tree["children"]:
            trees.append({"tree_id": tweet['id_str'], "tree_data": tree})
    if trees:
        try:
            user_trees.insert_many(trees)
        except pymongo.errors.BulkWriteError as e:
            print(f"Error inserting trees: {e.details}")
    pbar.update(len(batch))

# Thread worker function
def worker(queue, pbar):
    while True:
        batch = queue.get()
        if batch is None:
            queue.task_done()
            break
        process_batch(batch, pbar)
        queue.task_done()

# Initialize queue and threading
queue = Queue()
threads = []
num_threads = 4

# Main loop to process tweets in batches
batch_size = 10000
total_tweets = user_convo_starters.count_documents({})

# Start threads for processing
for _ in range(num_threads):
    thread = threading.Thread(target=worker, args=(queue, tqdm(total=total_tweets, desc="Storing trees", leave=False)))
    thread.start()
    threads.append(thread)

# Process batches of tweets
with tqdm(total=total_tweets, desc="Fetching and Processing tweets") as pbar:
    for i in range(0, total_tweets, batch_size):
        batch = list(user_convo_starters.find().skip(i).limit(batch_size))
        queue.put(batch)
        pbar.update(len(batch))

    # Add None to the queue for each thread to signal them to exit
    for _ in range(num_threads):
        queue.put(None)

    # Wait for the queue to be fully processed
    queue.join()

# Wait for all threads to finish
for thread in threads:
    thread.join()

print("Processing complete.")
