from pymongo import MongoClient
import re
client = MongoClient("mongodb://localhost:27017/")

db = client['grouping_airlines']

#list of airline collections

airlines = [
    "KLM", "AirFrance", "British_Airways", "AmericanAir", "Lufthansa", 
    "AirBerlin", "easyJet", "RyanAir", "SingaporeAir", "Qantas", 
    "EtihadAirways", "VirginAtlantic"]

for airline in airlines:
    db.create_collection(airline)

# Replace 'your_collection' with the name of your original collection
original_collection = db['airline_validtrees']

# Mapping to handle different representations of the same airline
airline_mapping = {
    "KLM": "KLM",
    "AirFrance": "AirFrance",
    "British_Airways": "British_Airways",
    "AmericanAir": "AmericanAir",
    "Lufthansa": "Lufthansa",
    "AirBerlin": "AirBerlin",
    "easyJet": "easyJet",
    "RyanAir": "RyanAir",
    "SingaporeAir": "SingaporeAir",
    "Qantas": "Qantas",
    "EtihadAirways": "EtihadAirways",
    "VirginAtlantic": "VirginAtlantic",
    "British Airways": "British_Airways",
    "Etihad Airways": "EtihadAirways",
    "Royal Dutch Airlines": "KLM",
    "Ryanair": "RyanAir",
    "Singapore Airlines": "SingaporeAir",
    "Virgin Atlantic": "VirginAtlantic"
}

# Function to find the correct collection name based on user_name
def get_collection_name(user_name):
    user_name = user_name.replace(" ", "").replace("-", "").lower()
    for key, value in airline_mapping.items():
        if re.sub(r'\W', '', key).lower() in user_name:
            return value
    return None

# Fetch all documents from the original collection
documents = original_collection.find()

# Iterate through each document and move it to the new collection
for document in documents:
    # Extract the user_name to determine the target collection
    user_name = document.get('user_name')
    
    if user_name:
        # Get the correct collection name based on user_name
        target_collection_name = get_collection_name(user_name)
        
        if target_collection_name:
            # Insert the document into the target collection
            db[target_collection_name].insert_one(document)

print("Documents have been copied to respective collections.")