from datetime import datetime
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from tqdm import tqdm

def filter_tweets_by_date_in_large_batches(db: Database, source_collection_name: str, new_collection_name: str, start_date_str: str, end_date_str: str, batch_size: int = 20000) -> None:
    """
    Filters tweets in the source collection in large batches based on the tweet's creation date
    and creates a new collection with these filtered tweets.

    Parameters:
    db (Database): The MongoDB database instance.
    source_collection_name (str): The name of the source collection to filter.
    new_collection_name (str): The name of the new collection to store filtered tweets.
    start_date_str (str): The start date in 'YYYY-MM-DD' format.
    end_date_str (str): The end date in 'YYYY-MM-DD' format.
    batch_size (int, optional): Number of documents to process in each batch. Defaults to 20000.
    
    Returns:
    None
    """
    try:
        # Parse the start and end dates
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

        print(f"Start date: {start_date}")
        print(f"End date: {end_date}")

        # Connect to the source collection
        source_collection: Collection = db[source_collection_name]

        # Create a new collection (if it doesn't exist)
        new_collection: Collection = db[new_collection_name]
        new_collection.delete_many({})  # Clear the new collection if it already exists

        # Create an index on the 'id_str' field if it doesn't exist
        source_collection.create_index('id_str')

        # Initialize counters
        total_documents = source_collection.count_documents({})
        processed_documents = 0

        print(f"Total documents in source collection: {total_documents}")

        # Initialize the progress bar
        with tqdm(total=total_documents, desc="Filtering Tweets") as pbar:
            # Process tweets in large batches
            while processed_documents < total_documents:
                batch = []
                # Fetch a batch of documents
                for tweet in source_collection.find().skip(processed_documents).limit(batch_size):
                    created_at = tweet.get('created_at')
                    if created_at:
                        tweet_date = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y').replace(tzinfo=None)
                        if start_date <= tweet_date < end_date:
                            batch.append(tweet)
                # Insert batch into new collection
                if batch:
                    new_collection.insert_many(batch)
                # Update progress
                processed_documents += len(batch)
                pbar.update(len(batch))

        print(f"Tweets created between {start_date_str} and {end_date_str} have been copied to the collection '{new_collection_name}'.")

    except Exception as e:
        print(f"Error occurred: {str(e)}")

# Example usage:
client = MongoClient('mongodb://localhost:27017/')
db = client['AirplaneMode']  # Replace with your database name

filter_tweets_by_date_in_large_batches(db, 'Cleaned_data_complete', 'Timeframe_filtered_tweets', '2020-01-01', '2020-03-01', batch_size=20000)

# Close the MongoDB connection
client.close()
