

import pymongo
import json
from conversation_mining import remove_duplicates

def make_collection(path, db)
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client[db]
    collection = db['cleaned_data']


    with open(f'{path}/cleaned_data.json') as file:
        file_data = json.load(file)
        
    # Inserting the loaded data in the Collection
    # if JSON contains data more than one entry
    # insert_many is used else insert_one is used
    if isinstance(file_data, list):
        collection.insert_many(file_data)  
    else:
        collection.insert_one(file_data)
    client.close()