from pymongo import MongoClient

def connect_to_db():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['AirplaneMode']
    collection = db['removed_duplicates']
    return collection

def retrieve_and_store_fields(collection):
    query = {
        'user.id_str': {'$exists': True},  # Check if 'user.id_str' exists
        'in_reply_to_status_id_str': {'$exists': True},  # Check if 'in_reply_to_status_id_str' exists
        'id_str': {'$exists': True}  # Check if 'id_str' exists
    }
    projection = {
        '_id': 0,  # Exclude the '_id' field from the result
        'id_str': 1,  # Include the 'id_str' field
        'in_reply_to_status_id_str': 1,  # Include the 'in_reply_to_status_id_str' field
        'user.id_str': 1  # Include the 'user.id_str' field
    }

    cursor = collection.find(query, projection)

    # Connect to the database and create a new collection
    new_collection = collection.database['needed_fields']

    # Insert the retrieved fields into the new collection
    for document in cursor:
        new_collection.insert_one({
            'id_str': document['id_str'],
            'in_reply_to_status_id_str': document['in_reply_to_status_id_str'],
            'user_id_str': document['user']['id_str']
        })

    print("Data stored in new collection.")

def main():
    collection = connect_to_db()
    retrieve_and_store_fields(collection)

if __name__ == "__main__":
    main()
