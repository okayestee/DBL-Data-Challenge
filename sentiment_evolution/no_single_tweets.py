from pymongo import MongoClient, InsertOne
from tqdm import tqdm

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client.AirplaneMode

# Collections
no_inconsistency_collection = db.needed_fields
no_single_tweets_collection = db.no_single_tweets

# Ensure the no_single_tweets collection is empty
no_single_tweets_collection.drop()

# Function to fetch batch of documents
def fetch_batch(collection, query, projection, skip, limit):
    return list(collection.find(query, projection).skip(skip).limit(limit))

# Function to filter and insert tweets into the no_single_tweets collection
def filter_and_insert_tweets(batch_size, total_docs):
    for skip in range(0, total_docs, batch_size):
        batch = fetch_batch(no_inconsistency_collection, {}, {'_id': 0}, skip, batch_size)
        if not batch:
            break
        filtered_tweets = [
            doc for doc in batch
            if not (doc.get('in_reply_to_status_id_str') is None and doc.get('counted_reply', 0) == 0)
        ]
        if filtered_tweets:
            requests = [InsertOne(doc) for doc in filtered_tweets]
            no_single_tweets_collection.bulk_write(requests)
            yield len(filtered_tweets)

# Main function to filter and insert tweets into the no_single_tweets collection
def main():
    batch_size = 10000

    # Get total count of documents in no_inconsistency collection
    total_docs = no_inconsistency_collection.count_documents({})
    print("Total documents:", total_docs)

    # Filter and insert tweets with replies into the no_single_tweets collection
    with tqdm(total=total_docs, desc="Filtering and inserting tweets") as pbar:
        for count in filter_and_insert_tweets(batch_size, total_docs):
            pbar.update(count)

if __name__ == "__main__":
    main()

# Close the connection
client.close()
