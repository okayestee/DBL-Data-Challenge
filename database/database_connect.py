import pymongo
import subprocess
import os

#directory containing JSON files to import
directory = "/Users/esteerutten/DataScienceFiles/DBL Data Challenge Map/data airlines"

#connection details

mongo_uri = "mongodb://localhost:27017/"
database_name ="DBL2" #name of the database you wanna connect to 

# Connect to MongoDB
client = pymongo.MongoClient(mongo_uri)
database = client[database_name]

# Perform operations on the database
collection = database["mycollection"] #insert name of the collection
collection.insert_one({"key": "value"})

# MongoDB Compass CLI command for importing JSON files
compass_command = 'mongodb-compass import --file {} --collection {} --uri "{}{}"'

# Iterate over JSON files in the directory
for filename in os.listdir(directory):
    if filename.endswith('.json'):
        filepath = os.path.join(directory, filename)
        collection_name = os.path.splitext(filename)[0]  # Use file name as collection name
        command = f'mongoimport --uri "{mongo_uri}" --db {database_name} --collection {collection_name} --file {filepath} --jsonArray'
        subprocess.run(command, shell=True)