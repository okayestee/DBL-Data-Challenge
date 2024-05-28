#setup mongoDB connection
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['AirplaneMode'] #your database
tweets_collection = db['Cleaned_data_complete'] #your collection within database

removed_dupl = db['removed_duplicates']  # Collection where duplicates are removed
# Identify duplicates pipeline
pipeline = [
    {"$group": {
        "_id": "$id_str",
        "uniqueIds": {"$addToSet": "$_id"},
        "count": {"$sum": 1}
    }},
    {"$match": {
        "count": {"$gt": 1}
    }}
]

duplicates = list(tweets_collection.aggregate(pipeline))

# Remove duplicates, keeping only one of each
for duplicate in duplicates:
    unique_ids = duplicate['uniqueIds']
    keep_id = unique_ids.pop(0)  # Keep the first occurrence
    
    # Insert the document to keep into the new collection
    document_to_keep = tweets_collection.find_one({"_id": keep_id})
    removed_dupl.insert_one(document_to_keep)

# Insert documents that were not part of duplicates
non_duplicates_pipeline = [
    {"$group": {
        "_id": "$id_str",
        "uniqueIds": {"$addToSet": "$_id"},
        "count": {"$sum": 1}
    }},
    {"$match": {
        "count": {"$eq": 1}
    }}
]

non_duplicates = list(tweets_collection.aggregate(non_duplicates_pipeline))

for non_duplicate in non_duplicates:
    doc_id = non_duplicate['uniqueIds'][0]
    document_to_keep = tweets_collection.find_one({"_id": doc_id})
    removed_dupl.insert_one(document_to_keep)


#test



#airline_keywords = ['airline', 'flight', 'airport', 'airplane', 'plane', 'boarding', 'departure', 'arrival'] 
#airline_tags = ['@AmericanAir']

#create indexes on relevant fields, speeds up the matching process
#tweets_collection.create_index([('text', 'text')])
#tweets_collection.create_index([('entities.user_mentions.screen_name', 1)])
#tweets_collection.create_index([('media', 1)])
##tweets_collection.create_index([('links', 1)])

#pipeline = [
    #Filter tweets based on keywords and airline mentions
   # {
  #      '$match' : {
   #         '$or' : [
   #             #match tweets with keywords in text
    #            { 'text': { '$regex': '|'.join(airline_keywords), '$options': 'i' } },
   #             #match tweets mentioning airline tags
    #                { 'entities.user_mentions.screen_name': { '$in': [tag[1:] for tag in airline_tags] } }
     #       ],
    #    }
   # }
#]

#filtered_tweets = list(tweets_collection.aggregate(pipeline))

#for tweet in filtered_tweets:
    #print(tweet)



