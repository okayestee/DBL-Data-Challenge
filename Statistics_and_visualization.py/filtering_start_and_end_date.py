from pymongo import MongoClient
from tqdm import tqdm
from pymongo.collection import Collection
from pymongo.database import Database
from datetime import datetime, timezone

def filter_and_create_collection(db: Database, source_collection_name: str, new_collection_name: str, start_date_str: str, end_date_str: str) -> None:
    start_date = datetime.strptime(start_date_str, '%a %b %d %H:%M:%S %z %Y').astimezone()
    end_date = datetime.strptime(end_date_str, '%a %b %d %H:%M:%S %z %Y').astimezone()

    source_collection = db[source_collection_name]
    new_collection = db[new_collection_name]
    new_collection.delete_many({})  # Clear the new collection if it already exists

    total_documents = source_collection.count_documents({})
    with tqdm(total=total_documents, desc="Filtering Trees") as pbar:
        for document in source_collection.find():
            tree_data = document.get('tree_data')
            if tree_data:
                root_tweet_data = tree_data.get('data')
                if root_tweet_data:
                    root_created_at = datetime.strptime(root_tweet_data['created_at'], '%a %b %d %H:%M:%S %z %Y').astimezone()
                    if start_date <= root_created_at <= end_date:
                        new_collection.insert_one(document)
            pbar.update(1)
