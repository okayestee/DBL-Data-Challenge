from pymongo import MongoClient

# Replace with your MongoDB connection string
client = MongoClient('mongodb://localhost:27017/')

# Select the AirplaneMode database and the Collection_no_single_tweets collection
db = client.AirplaneMode
collection = db.removed_duplicates

# Create a new collection for storing the inconsistent documents
new_collection = db.Inconsistent_Tweets
new_collection.drop()  # Ensure the new collection is empty

# Create indexes to improve query performance
collection.create_index('in_status_reply_to_id_str')
collection.create_index('in_reply_to_user_id_str')

# Define the query to find documents where in_status_reply_to_id_str is null but in_reply_to_user_id_str is not null
query = {
    'in_status_reply_to_id_str': None,
    'in_reply_to_user_id_str': {'$ne': None}
}

# Find the documents matching the query
inconsistent_docs = list(collection.find(query))

# Insert the inconsistent documents into the new collection
if inconsistent_docs:
    new_collection.insert_many(inconsistent_docs)
    print(f"Inserted {len(inconsistent_docs)} documents into the new collection 'Inconsistent_Tweets'.")

# Remove the inconsistent documents from the original collection
delete_result = collection.delete_many(query)
print(f"Deleted {delete_result.deleted_count} documents from 'Collection_no_single_tweets'.")

# Close the connection
client.close()

