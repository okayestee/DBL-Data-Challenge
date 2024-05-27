from pymongo import MongoClient, IndexModel, ASCENDING

def connect_to_db():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['AirplaneMode']
    collection = db['removed_duplicates']
    
    # Create indexes
    collection.create_indexes([
        IndexModel([('user.id_str', ASCENDING)]),
        IndexModel([('in_reply_to_status_id_str', ASCENDING)]),
        IndexModel([('id_str', ASCENDING)])
    ])
    
    return collection

def retrieve_and_store_fields(collection):
    query = {
        'user.id_str': {'$exists': True},
        'in_reply_to_status_id_str': {'$exists': True},
        'id_str': {'$exists': True}
    }
    projection = {
        '_id': 0,
        'id_str': 1,
        'in_reply_to_status_id_str': 1,
        'user.id_str': 1,
        'created_at': 1,
        'extended_tweet.full_text': 1,
        'text': 1
    }

    # Batch size for fetching documents
    batch_size = 10000

    # Connect to the database and create a new collection
    new_collection = collection.database['needed_fields']

    # Retrieve and store fields in batches
    cursor = collection.find(query, projection).batch_size(batch_size)
    for document in cursor:
        text = document.get('extended_tweet', {}).get('full_text') or document.get('text', '')
        new_document = {
            'id_str': document['id_str'],
            'in_reply_to_status_id_str': document['in_reply_to_status_id_str'],
            'user_id_str': document['user']['id_str'],
            'created_at': document.get('created_at', ''),
            'text': text
        }
        new_collection.insert_one(new_document)

    print("Data stored in new collection.")

def main():
    collection = connect_to_db()
    retrieve_and_store_fields(collection)

if __name__ == "__main__":
    main()
