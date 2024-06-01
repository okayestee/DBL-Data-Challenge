from pymongo import MongoClient
from treelib import Tree

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['DBL2']
user_trees_collection = db['airline_trees']
valid_trees_collection = db['valid_trees']

# List of airline IDs
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
        
        if validate_conversation_order(tree, airline_ids):
            trees.append(tree)
    
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

if __name__ == "__main__":
    main()



