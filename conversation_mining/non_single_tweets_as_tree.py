from pymongo import MongoClient, IndexModel, ASCENDING
from treelib import Tree

# Replace with your MongoDB connection string
client = MongoClient('mongodb://localhost:27017/')

# Select the AirplaneMode database and the Collection_no_single_tweets collection
db = client['AirplaneMode']
collection = db['Collection_no_single_tweets']

# Ensure indexes are created
collection.create_indexes([IndexModel([('id_str', ASCENDING)]), IndexModel([('in_reply_to_status_id_str', ASCENDING)])])

# Define the maximum number of documents to process
max_documents = 1664672  # Adjust this number as needed

# Initialize a dictionary to hold the tweet relationships
tweet_dict = {}

# Batch size for processing
batch_size = 10000
processed_count = 0

# Process tweets in batches
while processed_count < max_documents:
    cursor = collection.find({}, {'id_str': 1, 'user_id_str': 1, 'in_reply_to_status_id_str': 1, 'counted_reply': 1}).skip(processed_count).limit(batch_size)
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
def add_node(tweet_id, tree):
    if tweet_id in tree:
        return tree[tweet_id]

    tweet = tweet_dict[tweet_id]
    parent_id = tweet.get('in_reply_to_status_id_str')

    # Create a new node for the current tweet
    node = {
        "id_str": tweet['id_str'],
        "user_id_str": tweet['user_id_str'],
        "in_reply_to_status_id_str": tweet['in_reply_to_status_id_str'],
        "counted_reply": tweet['counted_reply'],
        "children": []
    }
    
    # If this tweet is a reply to another tweet and counted_reply is not 0
    if parent_id and parent_id in tweet_dict and tweet['counted_reply'] != 0:
        parent_node = add_node(parent_id, tree)
        parent_node["children"].append(node)
    else:
        # This is a root tweet or a leaf node with counted_reply == 0
        tree[tweet_id] = node

    return node

# Create trees for each root tweet
for tweet_id in tweet_dict:
    add_node(tweet_id, trees)

# Prepare a new collection for storing trees
trees_collection = db['Tweet_Trees']
trees_collection.drop()  # Drop the collection if it exists to start fresh

# Store each tree in the new collection
for root_id, tree in trees.items():
    trees_collection.insert_one({"root_id": root_id, "tree": tree})

# Close the connection
client.close()
