# from pymongo import MongoClient
# import pprint
# import treelib
# #only does the first 1000 documents
# # Establish a connection to the MongoDB server
# client = MongoClient('mongodb://localhost:27017/')

# # Access the database
# db = client['AirplaneMode']

# # Access the collection where duplicates are removed
# removed_dupl = db['no_inconsistency']

# # List of airline IDs as strings (assuming IDs are strings, change to integers if needed)
# airline_ids = ['56377143', '106062176', '18332190', '22536055', '124476322', 
#                '26223583', '2182373406', '38676903', '1542862735', '253340062', 
#                '45621423', '20626359', '218730857']

# def extract_airline_start(removed_dupl, airline_ids):
#     # Ensure all previous indexes are dropped
#     removed_dupl.drop_indexes()
    
#     # Create non-unique indexes
#     removed_dupl.create_index('user.id_str')
#     removed_dupl.create_index('in_reply_to_user_id_str')

#     # Define the new collection
#     airline_convo_starters = db['airline_convo_starters']

#     # Print sample documents to understand the structure
#     sample_doc = removed_dupl.find_one()
#     print("Sample document from 'removed_duplicates':")
#     pprint.pprint(sample_doc)

#     # Query to find documents that match an airline id and have a null 'in_reply_to_status_id_str'
#     query = {
#         'user.id_str': {'$in': airline_ids},
#         'in_reply_to_user_id_str': {'$in': [None, 'null']}  # Checking for None or 'null' string
#     }

#     # Debug: Print the query
#     print("Query to be executed:")
#     pprint.pprint(query)

#     # Use the explain method to verify index usage
#     execution_plan = removed_dupl.find(query).explain()
#     print("Execution plan:")
#     pprint.pprint(execution_plan)

#     # Find matching documents, limited to the first 1000 results
#     matching_docs = list(removed_dupl.find(query).limit(2781122))

#     # Debug: Print the number of matching documents found
#     print(f"Number of matching documents found: {len(matching_docs)}")
#     if matching_docs:
#         # Debug: Print some of the matching documents
#         for doc in matching_docs:
#             if doc.get('in_reply_to_status_id_str') not in [None, 'null']:
#                 print("Document with non-null in_reply_to_status_id_str found:")
#                 pprint.pprint(doc)
#             else:
#                 print("Valid document:")
#                 pprint.pprint(doc)

#         # Insert matching documents into the new collection if there are any
#         airline_convo_starters.insert_many(matching_docs)
#         print(f"Inserted {len(matching_docs)} documents into airline_convo_starters collection.")
#     else:
#         print("No matching documents found.")

# # Call the function to extract and insert the documents
# extract_airline_start(removed_dupl, airline_ids)



from pymongo import MongoClient
import pprint
from tqdm import tqdm

# Establish a connection to the MongoDB server
client = MongoClient('mongodb://localhost:27017/')

# Access the database
db = client['AirplaneMode']

# Access the collection where duplicates are removed
removed_dupl = db['no_inconsistency']

# List of airline IDs as strings (assuming IDs are strings, change to integers if needed)
airline_ids = ['56377143', '106062176', '18332190', '22536055', '124476322', 
               '26223583', '2182373406', '38676903', '1542862735', '253340062', 
               '45621423', '20626359', '218730857']

def extract_airline_start(removed_dupl, airline_ids):
    # Ensure all previous indexes are dropped
    removed_dupl.drop_indexes()
    
    # Create non-unique indexes
    removed_dupl.create_index('user.id_str')
    removed_dupl.create_index('in_reply_to_user_id_str')

    # Define the new collection
    airline_convo_starters = db['airline_convo_starters']

    # Print sample documents to understand the structure
    sample_doc = removed_dupl.find_one()
    print("Sample document from 'removed_duplicates':")
    pprint.pprint(sample_doc)

    # Query to find documents that match an airline id and have a null 'in_reply_to_status_id_str'
    query = {
        'user.id_str': {'$in': airline_ids},
        'in_reply_to_user_id_str': {'$in': [None, 'null']}  # Checking for None or 'null' string
    }

    # Debug: Print the query
    print("Query to be executed:")
    pprint.pprint(query)

    # Use the explain method to verify index usage
    execution_plan = removed_dupl.find(query).explain()
    print("Execution plan:")
    pprint.pprint(execution_plan)

    # Find matching documents, limited to the first 4,020,953 results
    cursor = removed_dupl.find(query).limit(4020953)

    # Create a progress bar using tqdm
    print("Fetching matching documents...")
    matching_docs = []
    for doc in tqdm(cursor, total=4020953, unit="documents"):
        matching_docs.append(doc)

    # Debug: Print the number of matching documents found
    print(f"Number of matching documents found: {len(matching_docs)}")
    if matching_docs:
        # Debug: Print some of the matching documents
        for doc in matching_docs[:5]:  # Print only the first 5 for brevity
            if doc.get('in_reply_to_status_id_str') not in [None, 'null']:
                print("Document with non-null in_reply_to_status_id_str found:")
                pprint.pprint(doc)
            else:
                print("Valid document:")
                pprint.pprint(doc)

        # Insert matching documents into the new collection if there are any
        airline_convo_starters.insert_many(matching_docs)
        print(f"Inserted {len(matching_docs)} documents into airline_convo_starters collection.")
    else:
        print("No matching documents found.")

# Call the function to extract and insert the documents
extract_airline_start(removed_dupl, airline_ids)

