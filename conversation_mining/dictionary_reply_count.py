# from pymongo import MongoClient

# #fetching tweets

# def fetch_tweet_ids(collection): 
#     '''
#     retrieves all tweet IDs from MongoDB
#     ''' 
#     cursor = collection.find({}, {'id': 1}) 
#     tweet_ids = [document['id']for document in cursor]
#     return tweet_ids

# def insert_removed_duplicates(tweet_ids, collection): 
#     '''
#     insert removed duplicates into another MongoDB collection
#     '''
#     removed_duplicates = [{'id': tweet_id, 'reply_count': 0} for tweet_id in tweet_ids]
#     collection.insert_many(removed_duplicates)

# def increment_reply_count(collection, tweet_id, increment_by=1): 
#     '''
#     increment reply count for a given tweet_id
#     '''
#     collection.update_one({'id': tweet_id}, {'$inc': {'reply_count': increment_by}})

# if __name__ == '__main__': 
#     client = MongoClient('mongodb://localhost:27017/')

#     db = client['AirplaneMode'] # your database
#     tweets_collection = db['removed_duplicates'] #existing collection

#     # db = client['AirplaneMode'] # Myungwon's database
#     # tweets_collection = db['removed_duplicates'] # Myungwon's existing collection

#     tweet_ids = fetch_tweet_ids(tweets_collection)

#     dict_count_replies = db['count_replies']
#     insert_removed_duplicates(tweet_ids, dict_count_replies)

#     # #practice test
#     # for doc in dict_count_replies.find().limit(50):
#     #     print(doc)

from pymongo import MongoClient, ASCENDING

def connect_to_db():
    # Connect to the MongoDB server (modify the URI as needed)
    client = MongoClient('mongodb://localhost:27017/')
    # Select the database
    db = client['AirplaneMode']
    # Select the collection
    collection = db['removed_duplicates']
    return db, collection

def remove_duplicates(collection):
    # Aggregate to find duplicate id_str
    pipeline = [
        {"$group": {"_id": "$id_str", "count": {"$sum": 1}, "ids": {"$push": "$_id"}}},
        {"$match": {"count": {"$gt": 1}}}
    ]
    
    duplicates = list(collection.aggregate(pipeline))
    
    for duplicate in duplicates:
        # Keep the first occurrence and remove the rest
        ids_to_remove = duplicate['ids'][1:]
        collection.delete_many({"_id": {"$in": ids_to_remove}})
        
    return len(duplicates)

def create_index_on_id_str(collection):
    # Create an index on the 'id_str' field
    index_name = collection.create_index([('id_str', ASCENDING)], unique=True)
    return index_name

def create_id_str_dict(collection):
    # Initialize an empty dictionary
    id_str_dict = {}
    # Process documents in batches to avoid the 16MB limit
    last_id = None
    batch_size = 10000

    while True:
        # Query to get a batch of documents
        if last_id:
            query = {'_id': {'$gt': last_id}}
        else:
            query = {}

        batch = list(collection.find(query).sort('_id', ASCENDING).limit(batch_size))
        
        if not batch:
            break

        for document in batch:
            id_str = document.get('id_str')
            if id_str and id_str not in id_str_dict:
                id_str_dict[id_str] = 0

        last_id = batch[-1]['_id']

    return id_str_dict

def store_data_in_new_collection(db, data):
    # Create a new collection
    new_collection = db['id_str_count']
    # Convert the dictionary to a list of documents
    documents = [{'id_str': key, 'count': value} for key, value in data.items()]
    # Insert the documents into the new collection
    new_collection.insert_many(documents)

def main():
    db, collection = connect_to_db()
    duplicate_count = remove_duplicates(collection)
    print(f"Removed {duplicate_count} duplicate(s).")
    create_index_on_id_str(collection)
    id_str_dict = create_id_str_dict(collection)
    store_data_in_new_collection(db, id_str_dict)
    print("Data stored in new collection 'id_str_count'.")

if __name__ == "__main__":
    main()
