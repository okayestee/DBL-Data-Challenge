from pymongo import MongoClient

# Replace with your MongoDB connection string
client = MongoClient('mongodb://localhost:27017/')

# Select the AirplaneMode database and the id_str_in_reply_to_status_id_str collection
db = client.AirplaneMode
collection = db.id_str_in_reply_to_status_id_str

# Query to find documents with counted_reply 0 and in_reply_to_status_id_str null
query = {
    "counted_reply": 0,
    "in_reply_to_status_id_str": None
}

# Find all documents matching the query
results = collection.find(query)

# Create or get the 'Collection_no_single_tweets' collection
collection_no_single_tweets = db.Collection_no_single_tweets

# Insert the filtered documents into the new collection
collection_no_single_tweets.insert_many(results)

# Close the connection
client.close()