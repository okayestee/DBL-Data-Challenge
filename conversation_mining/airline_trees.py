import pymongo
from pymongo import MongoClient, ASCENDING
from tqdm import tqdm
import threading
from queue import Queue

# Connect to MongoDB
def airline_trees():
    client = MongoClient("mongodb://localhost:27017/")
    db = client.DBL
    airline_convo_starters = db.airline_convo_starters
    replies = db.replies
    airline_trees = db.airline_trees

    # Create indexes to speed up the search
    airline_convo_starters.create_index([("id_str", ASCENDING)])
    replies.create_index([("in_reply_to_status_id_str", ASCENDING)])

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
    def process_batch(batch):
        for tweet in batch:
            tree = build_tree(tweet)
            if tree["children"]:
                airline_trees.insert_one({"tree_id": tweet['id_str'], "tree_data": tree})

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
    total_tweets = airline_convo_starters.count_documents({})

    with tqdm(total=total_tweets, desc="Processing tweets") as pbar:
        for i in range(0, total_tweets, batch_size):
            batch = list(airline_convo_starters.find().skip(i).limit(batch_size))
            queue.put(batch)
            pbar.update(len(batch))

        queue.join()

    # Stop workers
    for i in range(num_threads):
        queue.put(None)
    for thread in threads:
        thread.join()

    print("Processing complete.")

# Call the function to execute the script
airline_trees()
