import pymongo



#connection details

mongo_uri = "mongodb://localhost:27017/"
database_name ="mydatabase" #name of the database you wanna connect to 

# Connect to MongoDB
client = pymongo.MongoClient(mongo_uri)
database = client[database_name]

# Perform operations on the database
collection = database["mycollection"] #insert name of the collection
collection.insert_one({"key": "value"})