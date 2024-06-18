# from datetime import datetime
# from pymongo import MongoClient, IndexModel
# from pymongo.collection import Collection
# from pymongo.database import Database
# from tqdm import tqdm

# def filter_tweets_by_date_manually(db: Database, source_collection_name: str, new_collection_name: str, start_date_str: str, end_date_str: str) -> None:
#     """
#     Manually filters tweets in the source collection based on the tweet's creation date
#     and creates a new collection with these filtered tweets.

#     Parameters:
#     db (Database): The MongoDB database instance.
#     source_collection_name (str): The name of the source collection to filter.
#     new_collection_name (str): The name of the new collection to store filtered tweets.
#     start_date_str (str): The start date in 'YYYY-MM-DD' format.
#     end_date_str (str): The end date in 'YYYY-MM-DD' format.
    
#     Returns:
#     None
#     """
#     try:
#         # Parse the start and end dates
#         start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
#         end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

#         print(f"Start date: {start_date}")
#         print(f"End date: {end_date}")

#         # Connect to the source collection
#         source_collection: Collection = db[source_collection_name]

#         # Create a new collection (if it doesn't exist)
#         new_collection: Collection = db[new_collection_name]
#         new_collection.delete_many({})  # Clear the new collection if it already exists

#         # Create an index on the 'id_str' field if it doesn't exist
#         source_collection.create_index('id_str')

#         # Retrieve all documents from the source collection
#         total_documents = source_collection.count_documents({})
#         print(f"Total documents in source collection: {total_documents}")

#         # Initialize the progress bar
#         with tqdm(total=total_documents, desc="Filtering Tweets") as pbar:
#             # Iterate through each document and filter manually
#             for tweet in source_collection.find():
#                 created_at = tweet.get('created_at')
#                 if created_at:
#                     tweet_date = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y').replace(tzinfo=None)
#                     if start_date <= tweet_date < end_date:
#                         new_collection.insert_one(tweet)
#                 pbar.update(1)

#         print(f"Tweets created between {start_date_str} and {end_date_str} have been copied to the collection '{new_collection_name}'.")

#     except Exception as e:
#         print(f"Error occurred: {str(e)}")

# # Example usage:
# client = MongoClient('mongodb://localhost:27017/')
# db = client['AirplaneMode']  # Replace with your database name

# filter_tweets_by_date_manually(db, 'Cleaned_data_complete', 'Timeframe_filtered_tweets', '2020-01-01', '2020-03-01')

# # Close the MongoDB connection
# client.close()

from datetime import datetime
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from tqdm import tqdm

def filter_tweets_by_date_manually(db: Database, source_collection_name: str, new_collection_name: str, start_date_str: str, end_date_str: str) -> None:
    """
    Manually filters tweets in the source collection based on the tweet's creation date
    and creates a new collection with these filtered tweets.

    Parameters:
    db (Database): The MongoDB database instance.
    source_collection_name (str): The name of the source collection to filter.
    new_collection_name (str): The name of the new collection to store filtered tweets.
    start_date_str (str): The start date in 'YYYY-MM-DD' format.
    end_date_str (str): The end date in 'YYYY-MM-DD' format.
    
    Returns:
    None
    """
    try:
        # Parse the start and end dates
        start_date = datetime.strptime(start_date_str, '%a %b %d %H:%M:%S %z %Y')  # Parse the specific format
        end_date = datetime.strptime(end_date_str, '%a %b %d %H:%M:%S %z %Y')    # Parse the specific format

        print(f"Start date: {start_date}")
        print(f"End date: {end_date}")

        # Connect to the source collection
        source_collection: Collection = db[source_collection_name]

        # Create a new collection (if it doesn't exist)
        new_collection: Collection = db[new_collection_name]
        new_collection.delete_many({})  # Clear the new collection if it already exists

        # Create an index on the 'id_str' field if it doesn't exist
        source_collection.create_index('id_str')

        # Retrieve all documents from the source collection
        total_documents = source_collection.count_documents({})
        print(f"Total documents in source collection: {total_documents}")

        # Initialize the progress bar
        with tqdm(total=total_documents, desc="Filtering Tweets") as pbar:
            # Iterate through each document and filter manually
            for tweet in source_collection.find():
                created_at = tweet['created_at']
                if created_at:
                    tweet_date = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y')  # Parse to datetime
                    if start_date <= tweet_date <= end_date:
                        new_collection.insert_one(tweet)
                pbar.update(1)

        print(f"Tweets created between {start_date_str} and {end_date_str} have been copied to the collection '{new_collection_name}'.")

    except Exception as e:
        print(f"Error occurred: {str(e)}")

# Example usage:
client = MongoClient('mongodb://localhost:27017/')
db = client['AirplaneMode']  # Replace with your database name

start_date_str = "Wed Dec 23 00:00:00 +0000 2019"
end_date_str = "Wed Jan 01 00:00:00 +0000 2020"

filter_tweets_by_date_manually(db, 'Cleaned_data_complete', 'Timeframe_filtered_tweets', start_date_str, end_date_str)

# Close the MongoDB connection
client.close()
