from pymongo import MongoClient, InsertOne
from tqdm import tqdm

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client.AirplaneMode

# Collections
no_inconsistency_collection = db.no_inconsistency
needed_fields_collection = db.needed_fields

# Ensure the needed_fields collection is empty
needed_fields_collection.drop()

# Function to fetch batch of documents
def fetch_batch(collection, query, projection, skip, limit):
    return list(collection.find(query, projection).skip(skip).limit(limit))

# Function to extract and insert needed fields into the needed_fields collection
def extract_and_insert_needed_fields(batch_size, total_docs):
    for skip in range(0, total_docs, batch_size):
        batch = fetch_batch(no_inconsistency_collection, {}, {'_id': 0, 'id_str': 1, 'in_reply_to_status_id_str': 1, 'created_at': 1, 'text': 1, 'user.name': 1}, skip, batch_size)
        if not batch:
            break
        processed_docs = [
            {
                'id_str': doc['id_str'],
                'in_reply_to_status_id_str': doc.get('in_reply_to_status_id_str'),
                'created_at': doc['created_at'],
                'text': doc['text'],
                'user_name': doc['user']['name']
            }
            for doc in batch
        ]
        if processed_docs:
            requests = [InsertOne(doc) for doc in processed_docs]
            needed_fields_collection.bulk_write(requests)
            yield len(processed_docs)

# Main function to extract and insert needed fields into the needed_fields collection
def main():
    batch_size = 10000

    # Get total count of documents in no_inconsistency collection
    total_docs = no_inconsistency_collection.count_documents({})

    # Extract and insert needed fields into the needed_fields collection
    with tqdm(total=total_docs, desc="Extracting and inserting needed fields") as pbar:
        for count in extract_and_insert_needed_fields(batch_size, total_docs):
            pbar.update(count)

if __name__ == "__main__":
    main()

# Close the connection
client.close()
