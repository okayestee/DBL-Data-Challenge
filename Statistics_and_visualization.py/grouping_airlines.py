#group the airline trees (username)
#the user ones by mentioning an airline in their text

from pymongo import MongoClient

# Connect to your MongoDB server
client = MongoClient('mongodb://localhost:27017/')

# Replace 'your_database' with the name of your database
db = client['grouping_airlines']

# Replace 'your_collection' with the name of your original collection
original_collection = db['airline_validtrees']

# Fetch all documents from the original collection
documents = original_collection.find()

# Iterate through each document and move it to the new collection
for document in documents:
    # Extract the user_name to determine the target collection
    user_name = document.get('user_name')
    
    if user_name:
        # Define the target collection name based on user_name
        target_collection_name = user_name
        
        # Insert the document into the target collection
        db[target_collection_name].insert_one(document)

print("Documents have been copied to respective collections.")

