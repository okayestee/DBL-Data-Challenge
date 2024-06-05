from datetime import datetime
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['AirplaneMode']
valid_trees_collection = db['valid_trees_user']

# Specific airline user ID to check for
airline_id = '22536055'

def contains_specific_airline_user(tree_data, airline_id):
    # Set to keep track of visited nodes to prevent counting the same tree multiple times
    visited_nodes = set()

    # Recursive function to traverse through the children tweets
    def traverse_children(node):
        # Use node identifier to avoid duplicate counting
        node_id = id(node)
        if node_id in visited_nodes:
            return False

        visited_nodes.add(node_id)

        # Check if the current node has a user ID that matches the specific airline ID
        tweet_data = node.get('data')
        if tweet_data:
            user_data = tweet_data.get('user')
            if user_data and user_data.get('id_str') == airline_id:
                return True
        
        # Recursively traverse through the children nodes
        for child_node in node.get('children', []):
            for user, child_data in child_node.items():
                if traverse_children(child_data):
                    return True
        
        return False

    # Start traversing through the children tweets
    return traverse_children(tree_data)

def count_trees_with_specific_airline_user(collection, airline_id):
    count = 0
    # Iterate through all documents in the collection
    for document in collection.find():
        tree_data = document.get('tree_data')
        if tree_data and contains_specific_airline_user(tree_data, airline_id):
            count += 1
    return count

# Count the number of trees containing a child node with the specific airline user ID
trees_with_specific_airline_user_count = count_trees_with_specific_airline_user(valid_trees_collection, airline_id)

print(f"Number of trees containing a child node with the airline user ID {airline_id}: {trees_with_specific_airline_user_count}")
