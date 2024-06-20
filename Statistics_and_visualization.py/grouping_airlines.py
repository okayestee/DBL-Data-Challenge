from pymongo import MongoClient
import re
from tqdm import tqdm

client = MongoClient("mongodb://localhost:27017/")
db = client['DBL']

# List of airlines
airlines = [
    "KLM", "AirFrance", "British_Airways", "AmericanAir", "Lufthansa", 
    "AirBerlin", "easyJet", "RyanAir", "SingaporeAir", "Qantas", 
    "EtihadAirways", "VirginAtlantic"
]

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

# Create collections for each airline
for airline in airlines:
    db.create_collection(airline, capped=False)

# Function to find the correct collection name based on user_name
def get_collection_name(user_name):
    user_name = user_name.replace(" ", "").replace("-", "").lower()
    for key, value in airline_mapping.items():
        if re.sub(r'\W', '', key).lower() in user_name:
            return value
    return None

# Processing valid_trees_airline collection
original_collection = db['timeframe_trees_airline']

# Fetch all documents from the original collection
documents = original_collection.find()

# Iterate through each document and move it to the new collection
for document in documents:
    user_name = document.get('user_name')
    
    if user_name:
        target_collection_name = get_collection_name(user_name)
        
        if target_collection_name:
            db[target_collection_name].insert_one(document)

print("Documents from valid_trees_airline have been copied to respective collections.")

# Function to find the correct collection name based on text mention
def find_airline(text):
    text = text.lower()
    for alias, collection in airline_mapping.items():
        if re.search(f"@{alias.lower()}", text):
            return collection
    return None

# Recursive function to traverse and find mentions in the tree structure
def traverse_tree(node):
    if isinstance(node, dict):
        if 'data' in node and 'text' in node['data']:
            airline = find_airline(node['data']['text'])
            if airline:
                return airline
        for key in node:
            result = traverse_tree(node[key])
            if result:
                return result
    elif isinstance(node, list):
        for item in node:
            result = traverse_tree(item)
            if result:
                return result
    return None

# Processing valid_trees_user collection
original_collection = db['timeframe_trees_user']
documents = list(original_collection.find())

# Progress bar setup
total_docs = len(documents)
processed_docs = 0

# Iterate through each document and move it to the new collection
for document in tqdm(documents, desc="Processing Documents"):
    if 'tree_data' in document:
        airline = traverse_tree(document['tree_data'])
        if airline:
            db[airline].insert_one(document)
            processed_docs += 1

print(f"Total documents: {total_docs}")
print(f"Documents processed and moved: {processed_docs}")
if processed_docs < total_docs:
    print(f"Documents not processed: {total_docs - processed_docs}")

print("Documents from valid_trees_user have been copied to respective collections based on airline mentions.")
