# Setup MongoDB connection
from pymongo import MongoClient
import pprint

client = MongoClient('mongodb://localhost:27017/')
db = client['DBL2']  # your database 
removed_dupl = db['removed_duplicates']  # collection where duplicates are removed

# List of airline IDs
airline_ids = ['56377143', '106062176', '18332190', '22536055', '124476322', 
               '26223583', '2182373406', '38676903', '1542862735', '253340062', 
               '45621423', '20626359', '218730857']

def extract_airline_start(removed_dupl, airline_ids):
    # Ensure indexes are created
    removed_dupl.create_index([('id_str_1', 1)])
    removed_dupl.create_index([('in_status_reply_to_id_str_1', 1)])
 #user_id
    # Define the new collection
    airline_convo_starters = db['airline_convo_starters']

    # Query to find documents that match an airline id and the null value
    query = {
        'id_str_1': {'$in': airline_ids},
        'in_status_reply_to_id_str_1': None
    }

    # Use the explain method to verify index usage
    execution_plan = removed_dupl.find(query).explain()
    pprint.pprint(execution_plan)

    # Find matching documents
    matching_docs = list(removed_dupl.find(query))

    # Insert matching documents into the new collection
    if matching_docs:
        airline_convo_starters.insert_many(matching_docs)

# Call the function to extract and insert the documents
extract_airline_start(removed_dupl, airline_ids)

