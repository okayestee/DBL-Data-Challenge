from pymongo import MongoClient
from tqdm import tqdm

# Replace with your MongoDB connection string
client = MongoClient('mongodb://localhost:27017/')

# Select the AirplaneMode database and the needed_fields collection
db = client.AirplaneMode
collection = db.needed_fields

# Get the total number of documents in the collection
total_docs = collection.count_documents({})

# Define batch size
batch_size = 10000

# Process documents in batches
for skip in tqdm(range(0, total_docs, batch_size), desc="Processing batches"):
    # Dictionary to keep track of reply counts within the batch
    reply_counts = {}

    # Get documents for the current batch
    batch = collection.find({}, skip=skip, limit=batch_size)

    # Iterate through documents in the batch to count replies
    for doc in batch:
        in_reply_to_status_id_str = doc.get("in_reply_to_status_id_str")
        if in_reply_to_status_id_str:
            if in_reply_to_status_id_str in reply_counts:
                reply_counts[in_reply_to_status_id_str] += 1
            else:
                reply_counts[in_reply_to_status_id_str] = 1

    # Update documents in the batch with counted_reply field
    for doc in batch:
        id_str = doc.get("id_str")
        counted_reply = reply_counts.get(id_str, 0)
        collection.update_one(
            {"_id": doc["_id"]},
            {"$set": {"counted_reply": counted_reply}}
        )

# Close the connection
client.close()
