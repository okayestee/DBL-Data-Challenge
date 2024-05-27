from pymongo import MongoClient

def connect_to_db():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['AirplaneMode']
    collection = db['removed_duplicates']
    return db, collection

def create_id_str_dict(collection):
    id_str_dict = {}

    # Query to retrieve documents with id_str and in_reply_to_status_id_str fields
    query = {'id_str': {'$exists': True}, 'in_reply_to_status_id_str': {'$exists': True}}
    projection = {'id_str': 1, 'in_reply_to_status_id_str': 1}

    cursor = collection.find(query, projection)

    for document in cursor:
        id_str_dict[document['id_str']] = document['in_reply_to_status_id_str']

    return id_str_dict

def store_data_in_new_collection(db, data):
    new_collection = db['id_str_in_reply_to_status_id_str']
    new_collection.insert_many([{'id_str': key, 'in_reply_to_status_id_str': value} for key, value in data.items()])

def count_replies_and_update(db):
    collection = db['id_str_in_reply_to_status_id_str']
    reply_counts = {}

    # Iterate through all documents in the collection to count replies
    for doc in collection.find():
        in_reply_to_status_id_str = doc.get("in_reply_to_status_id_str")
        if in_reply_to_status_id_str:
            reply_counts[in_reply_to_status_id_str] = reply_counts.get(in_reply_to_status_id_str, 0) + 1

    # Now iterate through all documents again to update them with the counted_reply field
    for doc in collection.find():
        id_str = doc.get("id_str")
        counted_reply = reply_counts.get(id_str, 0)
        collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {"counted_reply": counted_reply}}
        )

def main():
    db, collection = connect_to_db()
    id_str_dict = create_id_str_dict(collection)
    store_data_in_new_collection(db, id_str_dict)
    count_replies_and_update(db)
    print("Data stored in new collection 'id_str_in_reply_to_status_id_str' and reply counts updated.")

if __name__ == "__main__":
    main()