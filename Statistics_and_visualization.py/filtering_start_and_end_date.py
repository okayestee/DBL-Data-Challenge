from pymongo import MongoClient
from tqdm import tqdm
from pymongo.collection import Collection
from pymongo.database import Database
from datetime import datetime, timezone

def filter_and_create_collection(db: Database, source_collection_name: str, new_collection_name: str, start_date_str: str, end_date_str: str) -> None:
    """
    Filters documents in the source collection where the root tweet's creation date
    is between the specified start and end dates, and creates a new collection with these filtered documents.

    Parameters:
    db (Database): The MongoDB database instance.
    source_collection_name (str): The name of the source collection to filter.
    new_collection_name (str): The name of the new collection to store filtered documents.
    start_date_str (str): The start date in the format 'Wed May 22 12:20:01 +0000 2019'.
    end_date_str (str): The end date in the format 'Fri May 24 12:20:01 +0000 2019'.
    
    Returns:
    None
    """
    # Parse the start and end dates and make them timezone-aware
    start_date = datetime.strptime(start_date_str, '%a %b %d %H:%M:%S %z %Y').astimezone(timezone.utc)
    end_date = datetime.strptime(end_date_str, '%a %b %d %H:%M:%S %z %Y').astimezone(timezone.utc)

    print(f"Start date: {start_date}")
    print(f"End date: {end_date}")

    # Connect to the source collection
    source_collection = db[source_collection_name]

    # Create a new collection
    new_collection = db[new_collection_name]
    new_collection.delete_many({})  # Clear the new collection if it already exists

    # Get the total number of documents in the source collection
    total_documents = source_collection.count_documents({})

    # Initialize the progress bar
    with tqdm(total=total_documents, desc="Filtering Trees") as pbar:
        # Iterate through the source collection and filter based on root node creation date
        for document in source_collection.find():
            tree_data = document.get('tree_data')
            if tree_data:
                root_tweet_data = tree_data.get('data')
                if root_tweet_data:
                    root_created_at = datetime.strptime(root_tweet_data['created_at'], '%a %b %d %H:%M:%S %z %Y').astimezone(timezone.utc)
                    if start_date <= root_created_at <= end_date:
                        # Insert the document into the new collection
                        new_collection.insert_one(document)
            
            # Update the progress bar
            pbar.update(1)
    
    print(f"Documents with root nodes created between {start_date_str} and {end_date_str} have been copied to the collection '{new_collection_name}'.")

# Example usage:
client = MongoClient('mongodb://localhost:27017/')
db = client['AirplaneMode']


start_date_str = "Wed May 22 12:20:01 +0000 2019"
end_date_str = "Wed Jan 01 12:20:01 +0000 2020"

filter_and_create_collection(db, 'valid_trees_user', 'timeframe_trees_user', start_date_str, end_date_str)
