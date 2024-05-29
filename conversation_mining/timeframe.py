from pymongo import MongoClient
from treelib import Tree, Node
from datetime import datetime, timedelta

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['AirplaneMode']
user_trees_collection = db['user_trees']

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

# Function to parse the 'created_at' field into a datetime object
def parse_created_at(tweet):
    return datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')

# Function to merge consecutive replies from the same user
def merge_consecutive_replies(tree):
    def merge_children(parent_id):
        children = tree.children(parent_id)
        merged_children = []
        i = 0
        
        while i < len(children):
            current_child = children[i]
            current_user_id = current_child.data['user']['id_str']
            merged_text = current_child.data['text']
            
            j = i + 1
            while j < len(children) and children[j].data['user']['id_str'] == current_user_id:
                merged_text += ' ' + children[j].data['text']
                j += 1
            
            # Keep the fields of the first reply, but merge the texts
            current_child.data['text'] = merged_text
            merged_children.append(current_child)
            i = j
        
        # Remove all original children nodes
        for child in children:
            tree.remove_node(child.identifier)
        
        # Re-add merged children
        for child in merged_children:
            tree.create_node(tag=child.tag, identifier=child.identifier, parent=parent_id, data=child.data)
        
        # Recur for each child
        for child in merged_children:
            merge_children(child.identifier)

    merge_children(tree.root)
    return tree

# Function to filter out tweets that do not respond within 24 hours
def filter_replies_within_24_hours(tree):
    def filter_children(parent_id):
        children = tree.children(parent_id)
        parent_created_at = parse_created_at(tree.get_node(parent_id).data)
        valid_children = []
        
        for child in children:
            child_created_at = parse_created_at(child.data)
            if child_created_at - parent_created_at <= timedelta(hours=24):
                valid_children.append(child)
                filter_children(child.identifier)  # Recur for valid children
            else:
                tree.remove_node(child.identifier)  # Remove invalid children
        
        # Ensure only valid children are kept
        for child in children:
            tree.remove_node(child.identifier)
        for child in valid_children:
            tree.create_node(tag=child.tag, identifier=child.identifier, parent=parent_id, data=child.data)
    
    filter_children(tree.root)
    return tree

def main():
    trees = []
    count = 0
    for tree_dict in user_trees_collection.find():
        tree = deserialize_tree(tree_dict)
        tree = merge_consecutive_replies(tree)
        tree = filter_replies_within_24_hours(tree)
        trees.append(tree)
        # Print the first 2 trees
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

