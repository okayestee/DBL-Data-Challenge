from pymongo import MongoClient, IndexModel, ASCENDING

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')

# Select the database and collections
db = client.DBL2
convo_starters_collection = db.airline_convo_starters
replies_collection = db.removed_duplicates

# Ensure indexes are created
convo_starters_collection.create_indexes([IndexModel([('id_str', ASCENDING)])])
replies_collection.create_indexes([IndexModel([('in_reply_to_status_id_str', ASCENDING)])])

# Define the maximum number of documents to process
max_documents = 10000  # Limit for testing, adjust as needed

# Initialize a dictionary to hold the tweet relationships
tweet_dict = {}

# Batch size for processing
batch_size = 1000
processed_count = 0

# Process conversation starters in batches
while processed_count < max_documents:
    cursor = convo_starters_collection.find({}, {'id_str': 1}).skip(processed_count).limit(batch_size)
    batch = list(cursor)
    
    if not batch:
        break
    
    # Add tweets to the dictionary
    for tweet in batch:
        tweet_dict[tweet['id_str']] = tweet
    
    processed_count += len(batch)
    print(f"Processed {processed_count}/{max_documents} conversation starters.")
    
    # If we've reached the max_documents limit, break out of the loop
    if processed_count >= max_documents:
        break

# Fetch replies and add them to the tweet_dict
cursor = replies_collection.find({}, {'id_str': 1, 'in_reply_to_status_id_str': 1})
for reply in cursor:
    tweet_dict[reply['id_str']] = reply

# Initialize a dictionary to keep track of trees
trees = {}

# Helper function to add nodes to a tree
def add_node(tweet_id, tree):
    if tweet_id in tree:
        return tree[tweet_id]

    tweet = tweet_dict[tweet_id]
    parent_id = tweet.get('in_reply_to_status_id_str')

    # Create a new node for the current tweet
    node = {"id_str": tweet_id, "children": []}
    
    # If this tweet is a reply to another tweet
    if parent_id and parent_id in tweet_dict:
        parent_node = add_node(parent_id, tree)
        parent_node["children"].append(node)
    else:
        # This is a root tweet
        tree[tweet_id] = node

    return node

# Create trees for each root tweet
for tweet_id in tweet_dict:
    add_node(tweet_id, trees)

# Prepare a new collection for storing trees
trees_collection = db.airline_roots
trees_collection.drop()  # Drop the collection if it exists to start fresh

# Store each tree in the new collection
for root_id, tree in trees.items():
    trees_collection.insert_one({"root_id": root_id, "tree": tree})

# Query to find replies using the $lookup aggregation
pipeline = [
    {
        '$lookup': {
            'from': 'removed_duplicates',
            'localField': 'root_id',
            'foreignField': 'in_reply_to_status_id_str',
            'as': 'replies'
        }
    },
    {
        '$project': {
            'root_id': 1,
            'tree': 1,
            'replies': 1
        }
    }
]

results = trees_collection.aggregate(pipeline)

# Print results
for root_tweet in results:
    print(f"Root Tweet ID: {root_tweet['root_id']}")
    for reply in root_tweet['replies']:
        print(f" - Reply: {reply['text']}")


# Select the database and collection
db = client.DBL2
trees_collection = db.airline_roots

# Helper function to recursively print the tree
def print_tree(node, level=0):
    indent = '  ' * level
    print(f"{indent}- Tweet ID: {node['id_str']}")
    for child in node['children']:
        print_tree(child, level + 1)

# Fetch and print two trees from the collection
cursor = trees_collection.find().limit(2)

for tree_document in cursor:
    print(f"Root Tweet ID: {tree_document['root_id']}")
    print_tree(tree_document['tree'])
    print("\n")


# Close the connection
client.close()



