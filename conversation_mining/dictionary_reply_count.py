from pymongo import MongoClient

def connect_to_db():
    # Connect to the MongoDB server (modify the URI as needed)
    client = MongoClient('mongodb://localhost:27017/')
    # Select the database
    db = client['AirplaneMode']
    # Select the collection
    collection = db['removed_duplicates']
    return db, collection

def create_id_str_dict(collection):
    # Initialize an empty dictionary
    id_str_dict = {}

    # Query to retrieve documents with id_str and in_reply_to_user_id_str fields
    query = {'id_str': {'$exists': True}, 'in_reply_to_user_id_str': {'$exists': True}}
    projection = {'id_str': 1, 'in_reply_to_user_id_str': 1, '_id': 0}

    # Iterate over documents and populate the dictionary
    for document in collection.find(query, projection):
        id_str_dict[document['id_str']] = document['in_reply_to_user_id_str']

    return id_str_dict

def store_data_in_new_collection(db, data):
    # Create a new collection
    new_collection = db['id_str_in_reply_to_user_id_str']
    # Convert the dictionary to a list of documents
    documents = [{'id_str': key, 'in_reply_to_user_id_str': value} for key, value in data.items()]
    # Insert the documents into the new collection
    new_collection.insert_many(documents)

def main():
    db, collection = connect_to_db()
    id_str_dict = create_id_str_dict(collection)
    store_data_in_new_collection(db, id_str_dict)
    print("Data stored in new collection 'id_str_in_reply_to_user_id_str'.")

if __name__ == "__main__":
    main()
