from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
from treelib import Tree
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['DBL2']
collection = db['replies']
starting_tweets_collection = db['user_roots']

# Create an index on the in_reply_to_status_id field
collection.create_index([('in_reply_to_status_id', ASCENDING)])

# Batch size
batch_size = 10000

# Lock for thread-safe progress bar update
lock = Lock()

def fetch_and_store_batch(skip, limit, pbar):
    # Fetch batch of tweets with in_reply_to_status_id as None
    tweets = list(collection.find({'in_reply_to_status_id': None}).skip(skip).limit(limit))
    
    # Insert batch into the new collection
    if tweets:
        try:
            starting_tweets_collection.insert_many(tweets, ordered=False)
        except BulkWriteError as bwe:
            print(bwe.details)
    
    # Update the progress bar
    with lock:
        pbar.update(len(tweets))

# Get the total number of tweets that match the criteria
total_count = collection.count_documents({'in_reply_to_status_id': None})

# Progress bar setup
pbar = tqdm(total=total_count)

# Use ThreadPoolExecutor for parallel processing
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(fetch_and_store_batch, skip, batch_size, pbar) for skip in range(0, total_count, batch_size)]
    
    # Ensure all futures are completed
    for future in as_completed(futures):
        future.result()

pbar.close()

# Function to connect to the MongoDB server and select collections
def connect_to_db():
    return db['user_roots'], collection, db['user_trees']

# Function to create indexes on relevant fields
def create_indexes(collection):
    collection.create_index([("user.id_str", ASCENDING)])
    collection.create_index([("in_reply_to_status_id_str", ASCENDING)])

# Function to build a single tree starting from a given root document
def build_tree_for_root(root_doc, replies_collection):
    tree = Tree()
    root_id = str(root_doc['_id'])
    tree.create_node(tag=root_id, identifier=root_id, data=root_doc)

    reply_map = {}
    for reply_doc in replies_collection.find():
        parent_id = str(reply_doc.get('in_reply_to_status_id_str'))
        if parent_id not in reply_map:
            reply_map[parent_id] = []
        reply_map[parent_id].append(reply_doc)
    
    def add_children(parent_id):
        if parent_id in reply_map:
            for child_doc in reply_map[parent_id]:
                child_id = str(child_doc['_id'])
                tree.create_node(tag=child_id, identifier=child_id, parent=parent_id, data=child_doc)
                add_children(child_id)
    
    add_children(root_id)
    return tree

# Function to serialize a tree to a dictionary
def serialize_tree(tree):
    def serialize_node(node):
        return {
            'id': node.identifier,
            'tag': node.tag,
            'data': node.data,
            'children': [serialize_node(child) for child in tree.children(node.identifier)]
        }
    root = tree.get_node(tree.root)
    return serialize_node(root)

# Function to store trees in the user_trees collection
def store_trees_in_new_collection(trees, user_trees_collection):
    serialized_trees = [serialize_tree(tree) for tree in trees]
    user_trees_collection.insert_many(serialized_trees)
    print(f"Inserted {len(serialized_trees)} trees into 'user_trees' collection.")

def main():
    user_roots_collection, replies_collection, user_trees_collection = connect_to_db()
    create_indexes(replies_collection)

    trees = []
    count = 0
    for root_doc in user_roots_collection.find():
        tree = build_tree_for_root(root_doc, replies_collection)
        trees.append(tree)
        # Print the first 2 trees
        if count < 2:
            print(f"Tree {count + 1}:")
            tree.show()
        count += 1

    store_trees_in_new_collection(trees, user_trees_collection)

if __name__ == "__main__":
    main()

client.close()




