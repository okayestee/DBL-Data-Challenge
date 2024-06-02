from pymongo import MongoClient
from tqdm import tqdm

# Step 1: Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['AirplaneMode']
collection = db['needed_fields']

# Step 2: Initialize a Dictionary for Counting Replies
reply_count = {}

# Step 3: First Iteration - Count Replies
total_documents = collection.count_documents({})
with tqdm(total=total_documents, desc="Counting replies") as pbar:
    for tweet in collection.find({}, {"in_reply_to_status_id_str": 1}):
        in_reply_to = tweet.get('in_reply_to_status_id_str')
        if in_reply_to:
            if in_reply_to in reply_count:
                reply_count[in_reply_to] += 1
            else:
                reply_count[in_reply_to] = 1
        pbar.update(1)

# Step 4: Second Iteration - Update Documents
with tqdm(total=total_documents, desc="Updating documents") as pbar:
    for tweet in collection.find({}, {"id_str": 1}):
        tweet_id = tweet.get('id_str')
        counted_reply = reply_count.get(tweet_id, 0)
        collection.update_one(
            {"_id": tweet["_id"]},
            {"$set": {"counted_reply": counted_reply}}
        )
        pbar.update(1)

print("Update completed.")
