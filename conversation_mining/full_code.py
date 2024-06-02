from pymongo import MongoClient
from treelib import Tree

from datetime import datetime, timedelta
from treelib import Node, Tree
#full filters including the building fo the trees
# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['DBL2']
user_trees_collection = db['airline_trees']
valid_trees_collection = db['valid_trees']
db = client['DBL_test']

# Define collection names for root documents and replies
root_collection_name = 'root'
reply_collection_name = 'replies'
new_tree_collection_name = 'new'

# Fetch root documents and replies
roots = list(db[root_collection_name].find())
replies = list(db[reply_collection_name].find())

# List of airline IDs
# Define the sequence of airline IDs
airline_ids = [
    '56377143', '106062176', '18332190', '22536055', '124476322', '26223583',
    '2182373406', '38676903', '1542862735', '253340062', '218730857',
    '45621423', '20626359'
]

# Function to deserialize a tree from a dictionary
# Function to deserialize a tree from a dictionary
def deserialize_tree(tree_dict):
    tree = Tree()

    def add_node(node_dict, parent=None):
        if 'id' in node_dict:
            node_id = node_dict['id']
        else:
            node_id = None

        if 'tag' in node_dict:
            tag = node_dict['tag']
        else:
            tag = None

        if 'data' in node_dict:
            data = node_dict['data']
        else:
            data = None

        tree.create_node(tag=tag, identifier=node_id, parent=parent, data=data)
        for child in node_dict.get('children', []):
            add_node(child, parent=node_id)

    add_node(tree_dict)
    return tree
def reorder_tweets(subtree, root_user_tweet):
    # Reorder tweets in the subtree based on the root tweet
    user_tweets = []
    airline_tweets = []

    for tweet in subtree:
        if tweet['id_str'] in airline_ids:
            airline_tweets.append(tweet)
        else:
            user_tweets.append(tweet)

# Function to validate the conversation order
def validate_conversation_order(tree, airline_ids):
    root = tree.get_node(tree.root)
    first_child = tree.children(root.identifier)[0] if tree.children(root.identifier) else None
    second_child = tree.children(first_child.identifier)[0] if first_child and tree.children(first_child.identifier) else None

    if first_child and second_child:
        first_user = root.data['user']['id_str']
        second_user = first_child.data['user']['id_str']
        third_user = second_child.data['user']['id_str']
    reordered_subtree = []
    if root_user_tweet:
        for user_tweet, airline_tweet in zip(user_tweets, airline_tweets):
            reordered_subtree.append(user_tweet)
            reordered_subtree.append(airline_tweet)
    else:
        for airline_tweet, user_tweet in zip(airline_tweets, user_tweets):
            reordered_subtree.append(airline_tweet)
            reordered_subtree.append(user_tweet)

        if (not is_airline(first_user) and is_airline(second_user) and not is_airline(third_user)) or \
           (is_airline(first_user) and not is_airline(second_user) and is_airline(third_user)):
            return True
    return reordered_subtree

    return False
def merge_replies(replies):
    # Merge replies from the same user
    merged_replies_dict = {}

    for reply in replies:
        user_id = reply['id_str']

        if user_id not in merged_replies_dict:
            merged_replies_dict[user_id] = reply
        else:
            # Merge text fields
            merged_replies_dict[user_id]['text'] += " " + reply['text']

            # Merge extended_tweet fields if they exist
            if 'extended_tweet' in reply and 'extended_tweet' in merged_replies_dict[user_id]:
                merged_replies_dict[user_id]['extended_tweet']['full_text'] += " " + reply['extended_tweet']['full_text']

            # Add any other necessary merge logic here for other fields

# Function to check if a user ID belongs to an airline
def is_airline(user_id):
    return user_id in airline_ids
    return list(merged_replies_dict.values())

def main():
    trees = []
    for tree_dict in user_trees_collection.find():
        tree = deserialize_tree(tree_dict)

        if validate_conversation_order(tree, airline_ids):
            trees.append(tree)
def get_replies(parent_id, parent_timestamp, root_user_tweet=None):
    # Get replies for the parent tweet
    reply_filter_criteria = {
        'in_reply_to_status_id_str': parent_id,
        # Additional criteria if needed
    }
    filtered_replies = [reply for reply in replies if reply_filter_criteria.items() <= reply.items()]

    # If there are valid trees, insert them into the collection
    if trees:
        # Clear the collection before inserting new trees
        valid_trees_collection.delete_many({})

        # Store the processed trees in the valid_trees collection
        serialized_trees = [deserialize_tree(tree) for tree in trees]
        valid_trees_collection.insert_many(serialized_trees)
        print(f"Inserted {len(serialized_trees)} trees into 'valid_trees' collection.")
    else:
        print("No valid trees found matching the defined order criteria.")
    # Filter replies to only include those within 24 hours of the parent tweet
    filtered_replies = [reply for reply in filtered_replies if 'created_at' in reply and datetime.strptime(reply['created_at'], '%a %b %d %H:%M:%S +0000 %Y') <= parent_timestamp + timedelta(hours=24)]

    # Merge replies from the same user
    merged_replies = merge_replies(filtered_replies)

    # Determine the order of tweets within the subtree based on the root tweet
    airline_order = root_user_tweet is not None and not root_user_tweet

    # Reorder replies according to the specified sequence
    reordered_replies = reorder_tweets(merged_replies, root_user_tweet)

    # Check the order of the first few tweets in the sequence
    if not check_order(reordered_replies[:4], airline_order):
        # Drop the rest of the tweets if the order is not followed
        reordered_replies = reordered_replies[:4]

    # Recursively fetch and attach child replies
    for reply in reordered_replies:
        reply_timestamp = datetime.strptime(reply['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
        reply['replies'] = get_replies(reply['id_str'], reply_timestamp, not airline_order)  # Invert order for child subtrees

    return reordered_replies

if __name__ == "__main__":
    main()
def check_order(tweets, airline_order):
    # Check the order of tweets based on the specified sequence
    if airline_order:
        # Check if the sequence follows airline-user-airline-user
        return all(tweet['id_str'] in airline_ids for tweet in tweets[::2]) and all(tweet['id_str'] not in airline_ids for tweet in tweets[1::2])
    else:
        # Check if the sequence follows user-airline-user-airline
        return all(tweet['id_str'] not in airline_ids for tweet in tweets[::2]) and all(tweet['id_str'] in airline_ids for tweet in tweets[1::2])

def build_tree(root):
    # Build a tree structure for a given root tweet
    tree = Tree()
    root_id = root['id_str']
    root_timestamp = datetime.strptime(root['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    tree.create_node(root_id, root_id, data=root)
    build_subtree(tree, root_id, root_timestamp, root['id_str'] not in airline_ids)
    return tree

def build_subtree(tree, parent_id, parent_timestamp, root_user_tweet=None):
    # Build a subtree for a given parent tweet
    replies = get_replies(parent_id, parent_timestamp, root_user_tweet)
    for reply in replies:
        reply_id = reply['id_str']
        reply_timestamp = datetime.strptime(reply['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
        reply_user_tweet = reply['id_str'] not in airline_ids
        tree.create_node(reply_id, reply_id, parent=parent_id, data=reply)
        build_subtree(tree, reply_id, reply_timestamp, reply_user_tweet)

def meets_all_requirements(tree):
    # Implement your logic to check if the tree meets all requirements
    # For example:
    # Check if the tree has a certain number of nodes
    if len(tree.all_nodes()) < 10:
        return False

    # Check if the order of tweets in the tree follows a specific pattern
    # Example logic:
    root_tweet = tree[tree.root].data
    if root_tweet['id_str'] in airline_ids:
        # Root tweet is from an airline
        expected_order = [True, False]  # Airline-user
    else:
        # Root tweet is from a user
        expected_order = [False, True]  # User-airline

    # Traverse the tree and check the order of tweets
    current_order = []
    for node_id in tree.expand_tree():
        tweet = tree[node_id].data
        if tweet['id_str'] in airline_ids:
            current_order.append(True)  # Airline tweet
        else:
            current_order.append(False)  # User tweet

    if current_order != expected_order:
        return False  # Order doesn't match

    # Add more requirements checks as needed

    # If all requirements are met, return True
    return True
