from pymongo import MongoClient
from tqdm import tqdm

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client.DBL

# Get collections
valid_trees_user = db.valid_trees_user
valid_trees_airline = db.valid_trees_airline
valid_trees_merged = db.valid_trees_merged

# Get total count for progress bar
total_documents = valid_trees_user.count_documents({}) + valid_trees_airline.count_documents({})

# Initialize progress bar
with tqdm(total=total_documents, desc="Merging collections") as pbar:
    # Merge collections
    for document in valid_trees_user.find():
        valid_trees_merged.insert_one(document)
        pbar.update(1)

    for document in valid_trees_airline.find():
        valid_trees_merged.insert_one(document)
        pbar.update(1)

print("Merge completed.")
