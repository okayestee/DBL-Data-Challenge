from pymongo import MongoClient
import pprint

# Establish a connection to the MongoDB server
client = MongoClient('mongodb://localhost:27017/')

# Access the database
db = client['DBL2']

# Access the collection where duplicates are removed
no_inconsistency = db['no_inconsistency']

# List of airline IDs as strings
airline_ids = [
    '56377143', '106062176', '18332190', '22536055', '124476322',
    '26223583', '2182373406', '38676903', '1542862735', '253340062',
    '45621423', '20626359', '218730857'
]

# Define batch size
BATCH_SIZE = 1000

def extract_airline_start(removed_dupl, airline_ids, batch_size):
    # Ensure all previous indexes are dropped
    removed_dupl.drop_indexes()
    
    # Create non-unique indexes
    removed_dupl.create_index('user.id_str')
    removed_dupl.create_index('in_reply_to_user_id_str')

    # Define the new collection
    airline_roots = db['airline_roots']

    # Print a sample document to understand the structure
    sample_doc = removed_dupl.find_one()
    print("Sample document from 'removed_dupl':")
    pprint.pprint(sample_doc)

    # Query to find documents that match an airline id and have a null 'in_reply_to_user_id_str'
    query = {
        'user.id_str': {'$in': airline_ids},
        'in_reply_to_user_id_str': {'$in': [None, 'null']}
    }

    # Debug: Print the query
    print("Query to be executed:")
    pprint.pprint(query)

    # Use the explain method to verify index usage
    execution_plan = removed_dupl.find(query).explain()
    print("Execution plan:")
    pprint.pprint(execution_plan)

    # Initialize counters and pagination
    total_docs = removed_dupl.count_documents(query)
    print(f"Total matching documents: {total_docs}")

    processed_docs = 0

    while processed_docs < total_docs:
        # Fetch documents in batches
        matching_docs = list(removed_dupl.find(query).skip(processed_docs).limit(batch_size))

        # Debug: Print the number of matching documents in the current batch
        print(f"Number of matching documents in batch: {len(matching_docs)}")

        if matching_docs:
            try:
                # Insert matching documents into the new collection
                airline_roots.insert_many(matching_docs)
                print(f"Inserted {len(matching_docs)} documents into 'airline_convo_starters' collection.")
            except Exception as e:
                print(f"Error inserting documents: {e}")

        # Update the count of processed documents
        processed_docs += len(matching_docs)

        # If the number of documents in the current batch is less than the batch size, break the loop
        if len(matching_docs) < batch_size:
            break

    print(f"Total processed documents: {processed_docs}")

# Call the function to extract and insert the documents
extract_airline_start(no_inconsistency, airline_ids, BATCH_SIZE)





