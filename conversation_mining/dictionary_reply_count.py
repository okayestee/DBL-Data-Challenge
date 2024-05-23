from pymongo import MongoClient, ASCENDING

def connect_to_db():
    # Connect to the MongoDB server (modify the URI as needed)
    client = MongoClient('mongodb://localhost:27017/')
    # Select the database
    db = client['DBL2']
    # Select the collection
    collection = db['removed_duplicates']
    return db, collection

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
    new_collection = db['id_str_count_test']
    # Convert the dictionary to a list of documents
    documents = [{'id_str': key, 'count': value} for key, value in data.items()]
    # Insert the documents into the new collection
    new_collection.insert_many(documents)



def compound_index(collection):
    '''
    Create a compound index on in_reply_to_status_id_str and id_str fields
    '''
    collection.create_index([('in_reply_to_status_id_str', ASCENDING), ('id_str', ASCENDING)])
    

def count_reply(collection):
    pass

"""
only wanted to run the compound index and collection functions

def main():
    db, collection = connect_to_db()
    create_index_on_id_str(collection)
    compound_index(collection)
    id_str_dict = create_id_str_dict(collection)
    store_data_in_new_collection(db, id_str_dict)

    print("Data stored in new collection 'id_str_count'.")
"""

def main():
    db, collection = connect_to_db()
    compound_index(collection)
    

if __name__ == "__main__":
    main()





