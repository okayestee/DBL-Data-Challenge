from pymongo import MongoClient
from treelib import Tree
from datetime import datetime

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['AirplaneMode']
user_trees_collection = db['user_trees']

# List of airline IDs
airline_ids = [
    '56377143', '106062176', '18332190', '22536055', '124476322', '26223583',
    '2182373406', '38676903', '1542862735', '253340062', '218730857',
    '45621423', '20626359'
]
"""
Summary of Changes:
Function to Validate Conversation Order:

validate_conversation_order: Checks if the initial sequence of tweets in each tree matches the defined patterns.
Uses the is_airline helper function to determine if a user is an airline based on their ID.
Validates two possible valid patterns for the beginning of the conversation.

"""
# Function to deserialize a tree from a dictionary
def deserialize_tree(tree_dict):
    tree = Tree()
    
    def add_node(node_dict, parent=None):
        tree.create_node(tag=node_dict['tag'], identifier=node_dict['id'], parent=parent, data=node_dict['data'])
        for child in node_dict['children']:
            add_node(child, parent=node_dict['id'])
    
    add_node(tree_dict)
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

# Function to validate the conversation order
def validate_conversation_order(tree, airline_ids):
    root = tree.get_node(tree.root)
    first_child = tree.children(root.identifier)[0] if tree.children(root.identifier) else None
    second_child = tree.children(first_child.identifier)[0] if first_child and tree.children(first_child.identifier) else None

    if first_child and second_child:
        first_user = root.data['user']['id_str']
        second_user = first_child.data['user']['id_str']
        third_user = second_child.data['user']['id_str']

        if (not is_airline(first_user) and is_airline(second_user) and not is_airline(third_user)):
            return True
        if (is_airline(first_user) and not is_airline(second_user) and is_airline(third_user)):
            return True

    return False

# Function to check if a user ID belongs to an airline
def is_airline(user_id):
    return user_id in airline_ids

def main():
    trees = []
    count = 0
    for tree_dict in user_trees_collection.find():
        tree = deserialize_tree(tree_dict)
        
        if validate_conversation_order(tree, airline_ids):
            trees.append(tree)
            # Print the first 2 valid trees
            if count < 2:
                print(f"Tree {count + 1}:")
                tree.show()
            count += 1
    
    # Clear the collection before inserting new trees
    user_trees_collection.delete_many({})
    
    # Store the processed trees in the user_trees collection
    serialized_trees = [serialize_tree(tree) for tree in trees]
    user_trees_collection.insert_many(serialized_trees)
    print(f"Inserted {len(serialized_trees)} trees into 'user_trees' collection.")

if __name__ == "__main__":
    main()

client.close()

