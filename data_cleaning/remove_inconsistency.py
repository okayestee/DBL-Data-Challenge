from pymongo import MongoClient, IndexModel, ASCENDING, errors
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def remove_inconsistencies():
    def connect_to_db():
        client = MongoClient('mongodb://localhost:27017/')
        db = client.DBL #Your database
        return db

    def create_indexes(collection):
        indexes = [
            IndexModel([('in_reply_to_status_id_str', ASCENDING)]),
            IndexModel([('in_reply_to_user_id_str', ASCENDING)])
        ]
        collection.create_indexes(indexes)

    def process_batch(original_collection, consistent_collection, query, skip, batch_size):
        cursor = original_collection.find(query).skip(skip).limit(batch_size)
        batch = list(cursor)

        if not batch:
            return 0

        try:
            consistent_collection.insert_many(batch, ordered=False)
            return len(batch)
        except errors.BulkWriteError as bwe:
            for error in bwe.details['writeErrors']:
                if error['code'] == 11000:
                    print(f"Duplicate key error on _id: {error['op']['_id']}")
                else:
                    raise
            return len(batch) - len(bwe.details['writeErrors'])

    def filter_and_store_consistent_data(db, max_documents, num_workers):
        original_collection = db.removed_duplicates
        consistent_collection = db.no_inconsistency
        consistent_collection.drop()

        query = {
            '$or': [
                {'in_reply_to_status_id_str': None, 'in_reply_to_user_id_str': None},
                {'in_reply_to_status_id_str': {'$ne': None}, 'in_reply_to_user_id_str': {'$ne': None}}
            ]
        }

        batch_size = 10000
        processed_count = 0

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = []
            with tqdm(total=max_documents, desc="Filtering and Storing Consistent Data", unit="documents") as pbar:
                for skip in range(0, max_documents, batch_size):
                    futures.append(executor.submit(process_batch, original_collection, consistent_collection, query, skip, batch_size))

                for future in as_completed(futures):
                    processed_batch = future.result()
                    processed_count += processed_batch
                    pbar.update(processed_batch)

        print(f"Stored {processed_count} consistent documents in the new collection.")

    max_documents = 4413045
    num_workers = 4 # Number of threads capped at 32 or 2x the number of CPU cores

    db = connect_to_db()
    filter_and_store_consistent_data(db, max_documents, num_workers)
    create_indexes(db.no_inconsistency)

remove_inconsistencies()


