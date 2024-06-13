from pymongo import MongoClient
from datetime import datetime, timedelta
from tqdm import tqdm

def timeframe_user_trees():
    # Define the threshold for 24 hours
    threshold_hours = 24

    # Connect to MongoDB and specify the collections
    client = MongoClient('mongodb://localhost:27017/')
    db = client['AirplaneMode']
    old_collection = db['user_trees']  # Your original collection with tree structures
    new_collection = db['timevertical_trees_user']  # New collection for filtered tree structures

    def filter_and_copy_tree(document):
        tree_data = document.get('tree_data', {})
        
        # Extract the creation time of the root tweet
        root_created_at = None
        root_data = tree_data.get('data', {})  # Assuming the root data is directly under 'tree_data'
        if 'created_at' in root_data:
            root_created_at = datetime.strptime(root_data['created_at'], '%a %b %d %H:%M:%S %z %Y')

        if root_created_at is None:
            # Skip this document if root_created_at is not available
            return None

        filtered_tree = {'tree_id': document['tree_id'], 'data': root_data.copy()}
        filtered_children = []
        parent_time = root_created_at
        
        # Iterate over the children of the root node
        for child in tree_data.get('children', []):  # Assuming children are directly under 'tree_data'
            child_created_at = None
            child_data = child.get('data', {})
            if 'created_at' in child_data:
                child_created_at = datetime.strptime(child_data['created_at'], '%a %b %d %H:%M:%S %z %Y')
            if child_created_at is None:
                # Skip this child if created_at is not available
                continue

            # Check if the child tweet was posted within 24 hours of its parent tweet
            if abs(parent_time - child_created_at).total_seconds() <= threshold_hours * 3600:
                filtered_children.append(child.copy())  # Add child if within 24 hours
                parent_time = child_created_at  # Update parent time to current child time

        if filtered_children:
            filtered_tree['children'] = filtered_children
            return filtered_tree
        else:
            return None

    # Count the total number of documents for the progress bar
    total_documents = old_collection.count_documents({})

    with tqdm(total=total_documents, desc="Filtering and Copying Trees") as pbar:
        for document in old_collection.find():
            filtered_document = filter_and_copy_tree(document.copy())
            if filtered_document:
                try:
                    new_collection.insert_one(filtered_document)
                except Exception as e:
                    print(f"Error inserting tree {filtered_document.get('tree_id', '')} into the new collection: {str(e)}")
            pbar.update(1)

    print("Processing complete.")

if __name__ == "__main__":
    timeframe_user_trees()
