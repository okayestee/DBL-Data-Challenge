'''
from pymongo import MongoClient
from datetime import datetime, timedelta
# horizontal
# Define the threshold for 24 hours
threshold_hours = 24

# Connect to MongoDB and specify the collections
client = MongoClient('mongodb://localhost:27017/')
db = client['DBL2']
old_collection = db['merge_trees']  # Your original collection with merged tweets
new_collection = db['time_filter']  # New collection for filtered tweets


def filter_and_copy_tree_horizontal(document):
    tree_data = document.get('tree_data', {})
    root_created_at = None
    root_data = tree_data.get(document['tree_id'], {}).get('data', {})
    if 'created_at' in root_data:
        root_created_at = datetime.strptime(root_data['created_at'], '%a %b %d %H:%M:%S %z %Y')

    if root_created_at is None:
        print(f"Warning: 'created_at' field is missing for document with _id: {document.get('_id', '')}")
        return None

    filtered_tree = {'tree_id': document['tree_id'], 'data': root_data.copy()}
    filtered_children = []
    for child in tree_data.get(document['tree_id'], {}).get('children', []):
        child_created_at = None
        if 'created_at' in child:
            child_created_at = datetime.strptime(child['created_at'], '%a %b %d %H:%M:%S %z %Y')
        if child_created_at is None:
            print(f"Warning: 'created_at' field is missing for a child of document with _id: {document.get('_id', '')}")
            continue

        if abs(root_created_at - child_created_at).total_seconds() <= threshold_hours * 3600:
            filtered_children.append(child.copy())  # Add child if within 24 hours

    if filtered_children:
        filtered_tree['children'] = filtered_children
        return filtered_tree
    else:
        print(f"Document with _id {document.get('_id', '')} has no children within 24 hours, skipping...")
        return None


for document in old_collection.find():
    filtered_document = filter_and_copy_tree_horizontal(document.copy())
    if filtered_document:
        print("Filtered document created, inserting...")
        try:
            new_collection.insert_one(filtered_document)
            print(f"Document {filtered_document.get('_id', '')} inserted into the new collection.")
        except Exception as e:
            print(f"Error inserting document {filtered_document.get('_id', '')} into the new collection: {str(e)}")
'''
'''
#vertical
from pymongo import MongoClient
from datetime import datetime, timedelta

# Define the threshold for 24 hours
threshold_hours = 24

# Connect to MongoDB and specify the collections
client = MongoClient('mongodb://localhost:27017/')
db = client['DBL2']
old_collection = db['merge_trees']  # Your original collection with merged tweets
new_collection = db['filtered_trees']  # New collection for filtered trees

def filter_and_copy_tree(document):
    tree_data = document.get('tree_data', {})
    root_created_at = None
    root_data = tree_data.get(document['tree_id'], {}).get('data', {})
    if 'created_at' in root_data:
        root_created_at = datetime.strptime(root_data['created_at'], '%a %b %d %H:%M:%S %z %Y')

    if root_created_at is None:
        print(f"Warning: 'created_at' field is missing for document with _id: {document.get('_id', '')}")
        return None

    filtered_tree = {'tree_id': document['tree_id'], 'data': root_data.copy()}
    filtered_children = []
    parent_time = root_created_at
    for child in tree_data.get(document['tree_id'], {}).get('children', []):
        child_created_at = None
        if 'created_at' in child:
            child_created_at = datetime.strptime(child['created_at'], '%a %b %d %H:%M:%S %z %Y')
        if child_created_at is None:
            print(f"Warning: 'created_at' field is missing for a child of document with _id: {document.get('_id', '')}")
            continue

        if abs(parent_time - child_created_at).total_seconds() <= threshold_hours * 3600:
            filtered_children.append(child.copy())  # Add child if within 24 hours
            parent_time = child_created_at  # Update parent time to current child time

    if filtered_children:
        filtered_tree['children'] = filtered_children
        return filtered_tree
    else:
        print(f"Document with _id {document.get('_id', '')} has no children within 24 hours, skipping...")
        return None

for document in old_collection.find():
    filtered_document = filter_and_copy_tree(document.copy())
    if filtered_document:
        print("Filtered document created, inserting...")
        try:
            new_collection.insert_one(filtered_document)
            print(f"Document {filtered_document.get('_id', '')} inserted into the new collection.")
        except Exception as e:
            print(f"Error inserting document {filtered_document.get('_id', '')} into the new collection: {str(e)}")
'''

from pymongo import MongoClient
from datetime import datetime, timedelta

def timeframe():
    # Define the threshold for 24 hours
    threshold_hours = 24

    # Connect to MongoDB and specify the collections
    client = MongoClient('mongodb://localhost:27017/')
    db = client['DBL2']
    old_collection = db['airline_trees2']  # Your original collection with merged tweets
    new_collection = db['timevertical_trees']  # New collection for filtered trees

    def filter_and_copy_tree(document):
        tree_data = document.get('tree_data', {})

        # Extract the creation time of the root tweet
        root_created_at = None
        root_data = tree_data.get(document['tree_id'], {}).get('data', {})
        if 'created_at' in root_data:
            root_created_at = datetime.strptime(root_data['created_at'], '%a %b %d %H:%M:%S %z %Y')

        if root_created_at is None:
            print(f"Warning: 'created_at' field is missing for document with _id: {document.get('_id', '')}")
            return None

        filtered_tree = {'tree_id': document['tree_id'], 'data': root_data.copy()}
        filtered_children = []
        parent_time = root_created_at

        # Iterate over the children of the root node
        for child_index, child in enumerate(tree_data.get(document['tree_id'], {}).get('children', [])):
            child_created_at = None
            child_data = child.get('data', {})
            if 'created_at' in child_data:
                child_created_at = datetime.strptime(child_data['created_at'], '%a %b %d %H:%M:%S %z %Y')
            if child_created_at is None:
                print(f"Warning: 'created_at' field is missing for a child of document with _id: {document.get('_id', '')}")
                continue

            # Check if the child tweet was posted within 24 hours of its parent tweet
            if abs(parent_time - child_created_at).total_seconds() <= threshold_hours * 3600:
                filtered_children.append(child.copy())  # Add child if within 24 hours
                parent_time = child_created_at  # Update parent time to current child time
            else:
                # Log the timestamp of the discarded child
                print(f"Discarded child timestamp: {child_created_at}")

        if filtered_children:
            filtered_tree['children'] = filtered_children
            return filtered_tree
        else:
            print(f"Document with _id {document.get('_id', '')} has no children within 24 hours, skipping...")
            return None

    for document in old_collection.find():
        filtered_document = filter_and_copy_tree(document.copy())
        if filtered_document:
            print("Filtered document created, inserting...")
            try:
                new_collection.insert_one(filtered_document)
                print(f"Document {filtered_document.get('_id', '')} inserted into the new collection.")
            except Exception as e:
                print(f"Error inserting document {filtered_document.get('_id', '')} into the new collection: {str(e)}")

# Call the function to execute the script
timeframe()
