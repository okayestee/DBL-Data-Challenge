import pymongo
import json
from pymongo import MongoClient, InsertOne
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

