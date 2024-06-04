from pymongo import MongoClient
from treelib import Tree
import json
from tqdm import tqdm
from collections import Counter
import matplotlib.pyplot as plt

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['AirplaneMode']
user_trees_collection = db['timevertical_trees_user']
valid_trees_collection = db['valid_trees_user']

# List of airline IDs
airline_ids = [
    '56377143', '106062176', '18332190', '22536055', '124476322', '26223583',
    '2182373406', '38676903', '1542862735', '253340062', '218730857',
    '45621423', '20626359'
]

# Function to deserialize a tree from a dictionary
def deserialize_tree(tree_dict):
    if not tree_dict:
        return None

    tree = Tree()

    def add_nodes(parent_id, node_data):
        node_id = node_data['data']['id_str']
        node_name = 'tree_data' if parent_id is None else node_data['data']['user'].get('name', 'Unknown Name')
        tree.create_node(tag=node_name, identifier=node_id, parent=parent_id, data=node_data['data'])
        for child in node_data.get('children', []):
            add_nodes(node_id, child)

    try:
        add_nodes(None, tree_dict)
    except Exception as e:
        print(f"Error while deserializing tree: {e}")
        return None

    return tree

# Function to validate the conversation order
def validate_conversation_order(tree, airline_ids):
    def has_airline_in_descendants(node_id):
        for child in tree.children(node_id):
            if is_airline(child.data['user']['id_str']):
                return True
            if has_airline_in_descendants(child.identifier):
                return True
        return False

    root = tree.get_node(tree.root)
    root_user_id = root.data['user']['id_str']

    if is_airline(root_user_id):
        # If root is an airline, check the first three users
        first_child = tree.children(root.identifier)[0] if tree.children(root.identifier) else None
        if not first_child:
            return False

        second_child = tree.children(first_child.identifier)[0] if tree.children(first_child.identifier) else None
        if not second_child:
            return False

        third_child = tree.children(second_child.identifier)[0] if second_child and tree.children(second_child.identifier) else None

        first_user = first_child.data['user']['id_str']
        second_user = second_child.data['user']['id_str']

        if third_child:
            third_user = third_child.data['user']['id_str']
            return not is_airline(first_user) and is_airline(second_user) and not is_airline(third_user)

        return not is_airline(first_user) and is_airline(second_user)
    else:
        # If root is not an airline, validate each child
        for child in tree.children(root.identifier):
            if not is_airline(child.data['user']['id_str']) and not has_airline_in_descendants(child.identifier):
                return False
        return True

# Function to check if a user ID belongs to an airline
def is_airline(user_id):
    return user_id in airline_ids

# Function to get the length of the conversation
def get_conversation_length(tree):
    max_depth = 0

    def traverse(node, depth):
        nonlocal max_depth
        if depth > max_depth:
            max_depth = depth
        for child in tree.children(node.identifier):
            traverse(child, depth + 1)

    root = tree.get_node(tree.root)
    traverse(root, 1)
    return max_depth

def main():
    trees = []
    conversation_lengths = []
    total_trees = user_trees_collection.count_documents({})

    tree_dict_cursor = user_trees_collection.find()

    for tree_dict in tqdm(tree_dict_cursor, total=total_trees, desc="Processing Trees"):
        tree = deserialize_tree(tree_dict)

        if tree and validate_conversation_order(tree, airline_ids):
            tree_data = tree_dict.get('data', {})
            user_data = tree_data.get('user', {})
            user_name = user_data.get('name', 'Unknown User')

            serialized_tree = json.loads(tree.to_json(with_data=True))
            serialized_tree['user_name'] = user_name
            trees.append(serialized_tree)

            conversation_length = get_conversation_length(tree)
            if conversation_length > 2:  # Exclude conversations of length 2
                conversation_lengths.append(conversation_length)
        else:
            continue

    if trees:
        valid_trees_collection.delete_many({})
        valid_trees_collection.insert_many(trees)
        print(f"Inserted {len(trees)} trees into 'valid_trees' collection.")

    # Calculate and print statistics
    length_counts = Counter(conversation_lengths)
    print("Conversation Length Distribution:")
    for length, count in sorted(length_counts.items()):
        print(f"Length: {length}, Count: {count}")

    # Create bar chart
    lengths = list(length_counts.keys())
    counts = list(length_counts.values())

    plt.bar(lengths, counts)
    plt.xlabel('Conversation Length')
    plt.ylabel('Frequency')
    plt.title('Distribution of Conversation Lengths')
    plt.show()

if __name__ == "__main__":
    main()

client.close()


