from pymongo import MongoClient

# Replace with your MongoDB connection string
client = MongoClient('mongodb://localhost:27017/')

# Select the AirplaneMode database and the Collection_no_single_tweets collection
db = client.DBL2
collection = db.removed_duplicates

# Replace 'your_collection' with your actual collection name
original_collection = db.removed_duplicates

# Replace 'new_collection' with your new collection name
new_collection = db.no_inconsistency

# Construct the query
query = [
    {
        '$match': {
            '$or': [
                {
                    'in_status_reply_to_id_str': None, 
                    'in_reply_to_user_id_str': None
                }, {
                    'in_reply_to_user_id_str': {
                        '$ne': None
                    }
                }
            ]
        }
    }
]
       

# Find the documents that match the query
documents_to_move = list(original_collection.find(query))

# Insert the documents into the new collection
if documents_to_move:
    new_collection.insert_many(documents_to_move)


print(f"Moved {len(documents_to_move)} documents to the new collection.")

