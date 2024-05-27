from pymongo import MongoClient, IndexModel, ASCENDING
from treelib import Tree

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')

# Select the database and collection
db = client.DBL2
collection = db.airline_convo_starters

# Ensure indexes are created
collection.create_indexes([IndexModel([('id_str', ASCENDING)]), IndexModel([('in_reply_to_status_id_str', ASCENDING)])])

# Define the maximum number of documents to process
max_documents = 10000  # Limit for testing, adjust as needed

# Initialize a dictionary to hold the tweet relationships
tweet_dict = {}

# Batch size for processing
batch_size = 1000
processed_count = 0

# Process tweets in batches
while processed_count < max_documents:
    cursor = collection.find({}, {'id_str': 1, 'in_reply_to_status_id_str': 1}).skip(processed_count).limit(batch_size)
    batch = list(cursor)
    
    if not batch:
        break
    
    # Add tweets to the dictionary
    for tweet in batch:
        tweet_dict[tweet['id_str']] = tweet
    
    processed_count += len(batch)
    print(f"Processed {processed_count}/{max_documents} documents.")
    
    # If we've reached the max_documents limit, break out of the loop
    if processed_count >= max_documents:
        break

# Initialize a dictionary to keep track of trees
trees = {}

# Helper function to add nodes to a tree
def add_node(tweet_id, tree, tree_obj):
    if tweet_id in tree:
        return tree[tweet_id]

    tweet = tweet_dict[tweet_id]
    parent_id = tweet.get('in_reply_to_status_id_str')

    # Create a new node for the current tweet
    node = {"id_str": tweet_id, "children": []}
    
    # Add node to the tree object
    tree_obj.create_node(tag=tweet_id, identifier=tweet_id)
    
    # If this tweet is a reply to another tweet
    if parent_id and parent_id in tweet_dict:
        parent_node = add_node(parent_id, tree, tree_obj)
        parent_node["children"].append(node)
        tree_obj.create_node(tag=tweet_id, identifier=tweet_id, parent=parent_id)
    else:
        # This is a root tweet
        tree[tweet_id] = node

    return node

# Create trees for each root tweet
for tweet_id in tweet_dict:
    tree_obj = Tree()
    add_node(tweet_id, trees, tree_obj)
    trees[tweet_id] = tree_obj

# Prepare a new collection for storing trees
trees_collection = db.Tweet_Trees
trees_collection.drop()  # Drop the collection if it exists to start fresh

# Store each tree in the new collection
for root_id, tree in trees.items():
    trees_collection.insert_one({"root_id": root_id, "tree": tree.to_dict(with_data=True)})

# Close the connection
client.close()
