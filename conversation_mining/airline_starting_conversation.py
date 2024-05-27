from pymongo import MongoClient

# Replace with your MongoDB connection string
client = MongoClient('mongodb://localhost:27017/')

# Select the AirplaneMode database and the Tweet_Trees collection
db = client['AirplaneMode']
trees_collection = db['Tweet_Trees']

# List of airline user IDs
airline_user_ids = [
    "56377143", "106062176", "18332190", "22536055", "124476322",
    "26223583", "2182373406", "38676903", "1542862735", "253340062",
    "218730857", "45621423", "20626359"
]

# Prepare a new collection for storing filtered trees
airline_starts_collection = db['Airline_starts']
airline_starts_collection.drop()  # Drop the collection if it exists to start fresh

# Filter trees based on the root node's user_id_str
filtered_trees = trees_collection.find({"tree.user_id_str": {"$in": airline_user_ids}})

# Insert filtered trees into Airline_starts collection
for tree in filtered_trees:
    airline_starts_collection.insert_one(tree)

# Close the connection
client.close()
