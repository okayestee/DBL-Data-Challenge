from datetime import datetime
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from tqdm import tqdm

def filter_tweets_by_date_manually(db: Database, source_collection_name: str, new_collection_name: str, start_date_str: str, end_date_str: str) -> None:
    start_date = datetime.strptime(start_date_str, '%a %b %d %H:%M:%S %z %Y')
    end_date = datetime.strptime(end_date_str, '%a %b %d %H:%M:%S %z %Y')

    source_collection: Collection = db[source_collection_name]
    new_collection: Collection = db[new_collection_name]
    new_collection.delete_many({})  # Clear the new collection if it already exists
    source_collection.create_index('id_str')

    total_documents = source_collection.count_documents({})
    with tqdm(total=total_documents, desc="Filtering Tweets") as pbar:
        for tweet in source_collection.find():
            created_at = tweet['created_at']
            if created_at:
                tweet_date = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y')
                if start_date <= tweet_date <= end_date:
                    new_collection.insert_one(tweet)
            pbar.update(1)
