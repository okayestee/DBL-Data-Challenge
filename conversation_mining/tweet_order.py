from pymongo import MongoClient
from treelib import Tree
import json

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['AirplaneMode']
user_trees_collection = db['timevertical_trees']
valid_trees_collection = db['valid_trees']

# List of airline IDs
airline_ids = [
    '56377143', '106062176', '18332190', '22536055', '124476322', '26223583',
    '2182373406', '38676903', '1542862735', '253340062', '218730857',
    '45621423', '20626359'
]

# Function to deserialize a tree from a dictionary
def deserialize_tree(tree_dict):
    tree = Tree()

    def add_nodes(parent_id, node_data):
        node_id = node_data['data']['id_str']
        tree.create_node(tag=node_data['data']['text'], identifier=node_id, parent=parent_id, data=node_data['data'])
        for child in node_data.get('children', []):
            add_nodes(node_id, child)

    add_nodes(None, tree_dict)
    return tree

# Function to validate the conversation order
def validate_conversation_order(tree, airline_ids):
    root = tree.get_node(tree.root)
    first_child = tree.children(root.identifier)[0] if tree.children(root.identifier) else None
    second_child = tree.children(first_child.identifier)[0] if first_child and tree.children(first_child.identifier) else None

    if first_child and second_child:
        first_user = root.data['user']['id_str']
        second_user = first_child.data['user']['id_str']
        third_user = second_child.data['user']['id_str']

        if (not is_airline(first_user) and is_airline(second_user) and not is_airline(third_user)) or \
           (is_airline(first_user) and not is_airline(second_user) and is_airline(third_user)):
            return True

    return False

# Function to check if a user ID belongs to an airline
def is_airline(user_id):
    return user_id in airline_ids

def main():
    trees = []
    for tree_dict in user_trees_collection.find():
        tree = deserialize_tree(tree_dict)
        
        if tree and validate_conversation_order(tree, airline_ids):
            # Retrieve user_name from the nested structure
            tree_data = tree_dict.get('tree_data', {})
            user_data = tree_data.get('data', {}).get('user', {})
            user_name = user_data.get('name', 'Unknown User')
            
            # Serialize the tree and add user_name field
            serialized_tree = json.loads(tree.to_json(with_data=True))
            serialized_tree['user_name'] = user_name
            trees.append(serialized_tree)
    
    # If there are valid trees, insert them into the collection
    if trees:
        # Clear the collection before inserting new trees
        valid_trees_collection.delete_many({})
        
        # Store the processed trees in the valid_trees collection
        valid_trees_collection.insert_many(trees)
        print(f"Inserted {len(trees)} trees into 'valid_trees' collection.")
    else:
        print("No valid trees found matching the defined order criteria.")

if __name__ == "__main__":
    main()

client.close()

# from pymongo import MongoClient
# from treelib import Tree
# import json

# # Connect to MongoDB
# client = MongoClient('mongodb://localhost:27017/')
# db = client['AirplaneMode']
# user_trees_collection = db['timevertical_trees']
# valid_trees_collection = db['valid_trees']

# # List of airline IDs
# airline_ids = [
#     '56377143', '106062176', '18332190', '22536055', '124476322', '26223583',
#     '2182373406', '38676903', '1542862735', '253340062', '218730857',
#     '45621423', '20626359'
# ]

# # Function to deserialize a tree from a dictionary
# def deserialize_tree(tree_dict):
#     if not tree_dict:
#         return None

#     tree = Tree()

#     def add_nodes(parent_id, node_data):
#         node_id = node_data['data']['id_str']
#         tree.create_node(tag=node_data['data']['text'], identifier=node_id, parent=parent_id, data=node_data['data'])
#         for child in node_data.get('children', []):
#             add_nodes(node_id, child)

#     try:
#         add_nodes(None, tree_dict)
#     except Exception as e:
#         print(f"Error while deserializing tree: {e}")
#         return None

#     return tree

# # Function to validate the conversation order
# def validate_conversation_order(tree, airline_ids):
#     root = tree.get_node(tree.root)
#     first_child = tree.children(root.identifier)[0] if tree.children(root.identifier) else None
#     second_child = tree.children(first_child.identifier)[0] if first_child and tree.children(first_child.identifier) else None

#     if first_child and second_child:
#         first_user = root.data['user']['id_str']
#         second_user = first_child.data['user']['id_str']
#         third_user = second_child.data['user']['id_str']

#         if (not is_airline(first_user) and is_airline(second_user) and not is_airline(third_user)) or \
#            (is_airline(first_user) and not is_airline(second_user) and is_airline(third_user)):
#             return True

#     return False

# # Function to check if a user ID belongs to an airline
# def is_airline(user_id):
#     return user_id in airline_ids

# def main():
#     trees = []
#     for tree_dict in user_trees_collection.find():
#         tree = deserialize_tree(tree_dict)
        
#         if tree and validate_conversation_order(tree, airline_ids):
#             # Retrieve user_name from the nested structure
#             tree_data = tree_dict.get('tree_data', {})
#             user_data = tree_data.get('data', {}).get('user', {})
#             user_name = user_data.get('name', 'Unknown User')
            
#             # Serialize the tree and add user_name field
#             serialized_tree = json.loads(tree.to_json(with_data=True))
#             serialized_tree['user_name'] = user_name
#             trees.append(serialized_tree)
#         else:
#             # Discard the tree if it doesn't meet the conversation order criteria
#             continue
        
#     # If there are valid trees, insert them into the collection
#     if trees:
#         # Clear the collection before inserting new trees
#         valid_trees_collection.delete_many({})
        
#         # Store the processed trees in the valid_trees collection
#         valid_trees_collection.insert_many(trees)
#         print(f"Inserted {len(trees)} trees into 'valid_trees' collection.")
#     else:
#         print("No valid trees found matching the defined order criteria.")

    
#     # If there are valid trees, insert them into the collection
#     if trees:
#         # Clear the collection before inserting new trees
#         valid_trees_collection.delete_many({})
        
#         # Store the processed trees in the valid_trees collection
#         valid_trees_collection.insert_many(trees)
#         print(f"Inserted {len(trees)} trees into 'valid_trees' collection.")
#     else:
#         print("No valid trees found matching the defined order criteria.")

# if __name__ == "__main__":
#     main()

# client.close()
