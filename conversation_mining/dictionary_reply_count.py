# from pymongo import MongoClient

# def connect_to_db():
#     client = MongoClient('mongodb://localhost:27017/')
#     db = client['AirplaneMode']
#     collection = db['removed_duplicates']
#     return db, collection

# def create_id_str_dict(collection):
#     id_str_dict = {}

#     # Query to retrieve documents with id_str and in_reply_to_status_id_str fields
#     query = {'id_str': {'$exists': True}, 'in_reply_to_status_id_str': {'$exists': True}}
#     projection = {'id_str': 1, 'in_reply_to_status_id_str': 1}

#     cursor = collection.find(query, projection)

#     for document in cursor:
#         id_str_dict[document['id_str']] = document['in_reply_to_status_id_str']

#     return id_str_dict

# def store_data_in_new_collection(db, data):
#     new_collection = db['id_str_in_reply_to_status_id_str']
#     new_collection.insert_many([{'id_str': key, 'in_reply_to_status_id_str': value} for key, value in data.items()])

# def main():
#     db, collection = connect_to_db()
#     id_str_dict = create_id_str_dict(collection)
#     store_data_in_new_collection(db, id_str_dict)
#     print("Data stored in new collection 'id_str_in_reply_to_status_id_str'.")

# if __name__ == "__main__":
#     main()

# from pymongo import MongoClient

# # Replace with your MongoDB connection string
# client = MongoClient('mongodb://localhost:27017/')

# # Select the AirplaneMode database and the id_str_in_reply_to_status_id_str collection
# db = client.AirplaneMode
# collection = db.id_str_in_reply_to_status_id_str

# # Dictionary to keep track of reply counts
# reply_counts = {}

# # Iterate through all documents in the collection
# for doc in collection.find():
#     in_reply_to_status_id_str = doc.get("in_reply_to_status_id_str")
#     if in_reply_to_status_id_str:
#         if in_reply_to_status_id_str in reply_counts:
#             reply_counts[in_reply_to_status_id_str] += 1
#         else:
#             reply_counts[in_reply_to_status_id_str] = 1

# # Now iterate through all documents again to update them with the counted_reply field
# for doc in collection.find():
#     id_str = doc.get("id_str")
#     counted_reply = reply_counts.get(id_str, 0)
#     collection.update_one(
#         {"_id": doc["_id"]},
#         {"$set": {"counted_reply": counted_reply}}
#     )

# # Close the connection
# client.close()

# from pymongo import MongoClient

# # Replace with your MongoDB connection string
# client = MongoClient('mongodb://localhost:27017/')

# # Select the AirplaneMode database and the id_str_in_reply_to_status_id_str collection
# db = client.AirplaneMode
# collection = db.id_str_in_reply_to_status_id_str

# # Query to find documents with counted_reply 0 and in_reply_to_status_id_str null
# query = {
#     "counted_reply": 0,
#     "in_reply_to_status_id_str": None
# }

# # Find all documents matching the query
# results = collection.find(query)

# # Create or get the 'Collection_no_single_tweets' collection
# collection_no_single_tweets = db.Collection_no_single_tweets

# # Insert the filtered documents into the new collection
# collection_no_single_tweets.insert_many(results)

# # Close the connection
# client.close()


from pymongo import MongoClient

# Replace with your MongoDB connection string
client = MongoClient('mongodb://localhost:27017/')

# Select the AirplaneMode database and the id_str_in_reply_to_status_id_str collection
db = client.AirplaneMode
collection = db.id_str_in_reply_to_status_id_str

# Query to find documents not meeting both conditions
query = {
    "$or": [
        {"counted_reply": {"$ne": 0}},
        {"in_reply_to_status_id_str": {"$ne": None}}
    ]
}

# Find all documents matching the query
results = collection.find(query)

# Create or get the 'Collection_no_single_tweets' collection
collection_no_single_tweets = db.Collection_no_single_tweets

# Insert the filtered documents into the new collection
collection_no_single_tweets.insert_many(results)

# Close the connection
client.close()
