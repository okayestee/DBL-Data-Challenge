from pymongo import MongoClient, IndexModel, ASCENDING
from treelib import Node, Tree

# Replace with your MongoDB connection string
client = MongoClient('mongodb://localhost:27017/')

# Select the AirplaneMode database and the appropriate collections
db = client['DBL2']
root_tweets_collection = db['RootTweets']
replies_collection = db['Replies']

# Ensure indexes are created for replies collection
indexes = [
    IndexModel([('id_str', ASCENDING)]),
    IndexModel([('in_reply_to_status_id_str', ASCENDING)])
]
replies_collection.create_indexes(indexes)

# Initialize a dictionary to hold tweet data
tweet_dict = {}

# Fetch all replies and store them in tweet_dict for quick lookup
for reply in replies_collection.find({}, {'id_str': 1, 'user_id_str': 1, 'in_reply_to_status_id_str': 1, 'counted_reply': 1}):
    tweet_dict[reply['id_str']] = reply

# Function to recursively add children to the tree
def add_children(tree, parent_id):
    for tweet_id, tweet in tweet_dict.items():
        if tweet.get('in_reply_to_status_id_str') == parent_id:
            tree.create_node(tweet['id_str'], tweet['id_str'], parent=parent_id, data=tweet)
            add_children(tree, tweet['id_str'])

# Prepare a new collection for storing tweet trees
trees_collection = db['Tweet_Trees']
trees_collection.drop()  # Drop the collection if it exists to start fresh

# Process each root tweet to build trees
for root_tweet in root_tweets_collection.find({}, {'id_str': 1, 'user_id_str': 1, 'counted_reply': 1}):
    tree = Tree()
    root_id = root_tweet['id_str']
    tree.create_node(root_id, root_id, data=root_tweet)
    
    # Recursively add children to the root node
    add_children(tree, root_id)
    
    # Store the tree structure in the new collection
    trees_collection.insert_one({"root_id": root_id, "tree": tree.to_dict(with_data=True)})

# Close the connection
client.close()


#Connect to MongoDB and fetch root tweets and replies
#Build a tree structure for each root tweet
#Store the trees in a new collection.