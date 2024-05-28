from pymongo import MongoClient

# Replace with your MongoDB connection string
client = MongoClient('mongodb://localhost:27017/')

# Select the AirplaneMode database and the id_str_in_reply_to_status_id_str collection
db = client['AirplaneMode']
collection = db['needed_fields']

# Dictionary to keep track of reply counts
reply_counts = {}

# Iterate through all documents in the collection
for doc in collection.find():
    in_reply_to_status_id_str = doc.get("in_reply_to_status_id_str")
    if in_reply_to_status_id_str:
        if in_reply_to_status_id_str in reply_counts:
            reply_counts[in_reply_to_status_id_str] += 1
        else:
            reply_counts[in_reply_to_status_id_str] = 1

# Now iterate through all documents again to update them with the counted_reply field
for doc in collection.find():
    id_str = doc.get("id_str")
    counted_reply = reply_counts.get(id_str, 0)
    collection.update_one(
        {"_id": doc["_id"]},
        {"$set": {"counted_reply": counted_reply}}
    )

# Close the connection
client.close()
