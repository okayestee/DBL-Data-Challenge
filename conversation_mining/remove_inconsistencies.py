from pymongo import MongoClient

# Replace with your MongoDB connection string
client = MongoClient('mongodb://localhost:27017/')

# Select the AirplaneMode database and the Collection_no_single_tweets collection
db = client.DBL
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




#If the above code does not work, make a comment of it and try this one below:
#Command for commenting and uncommenting the selected code is: ctrl + /




# from pymongo import MongoClient, IndexModel, ASCENDING, errors

# def connect_to_db():
#     client = MongoClient('mongodb://localhost:27017/')
#     db = client['AirplaneMode']
#     return db

# def filter_and_store_consistent_data(db):
#     original_collection = db.removed_duplicates
#     consistent_collection = db.no_inconsistency

#     # Ensure indexes for faster querying
#     indexes = [
#         IndexModel([('in_reply_to_status_id_str', ASCENDING)]),
#         IndexModel([('in_reply_to_user_id_str', ASCENDING)])
#     ]
#     original_collection.create_indexes(indexes)

#     # Construct the query to find consistent documents
#     query = {
#         '$or': [
#             {'in_reply_to_status_id_str': None, 'in_reply_to_user_id_str': None},
#             {'in_reply_to_status_id_str': {'$ne': None}, 'in_reply_to_user_id_str': {'$ne': None}}
#         ]
#     }

#     # Batch size for processing
#     batch_size = 10000
#     processed_count = 0

#     while True:
#         cursor = original_collection.find(query).skip(processed_count).limit(batch_size)
#         batch = list(cursor)

#         if not batch:
#             break

#         try:
#             # Insert the documents into the new collection
#             consistent_collection.insert_many(batch, ordered=False)  # ordered=False allows continuing on error
#         except errors.BulkWriteError as bwe:
#             # Handle duplicate key errors
#             for error in bwe.details['writeErrors']:
#                 if error['code'] == 11000:
#                     print(f"Duplicate key error on _id: {error['op']['_id']}")
#                 else:
#                     raise

#         processed_count += len(batch)
#         print(f"Processed {processed_count} documents.")

#     print(f"Stored {processed_count} consistent documents in the new collection.")

# def main():
#     db = connect_to_db()
#     filter_and_store_consistent_data(db)

# if __name__ == "__main__":
#     main()
