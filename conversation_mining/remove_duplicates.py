from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def remove_duplicates(db):
    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client.dbb  # Your database
    tweets_collection = db['cleaned_data']  # Your collection within database
    removed_dupl = db['removed_duplicates']  # New collection where duplicates are removed

    # Create index
    tweets_collection.create_index('user.id_str')

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

    # Function to process batches
    def process_batch(batch, is_duplicate):
        for document in batch:
            unique_ids = document['uniqueIds']
            keep_id = unique_ids.pop(0) if is_duplicate else unique_ids[0]

            document_to_keep = tweets_collection.find_one({"_id": keep_id})
            if document_to_keep:
                removed_dupl.insert_one(document_to_keep)

    # Get and process duplicates
    duplicates = list(tweets_collection.aggregate(pipeline))
    total_duplicates = len(duplicates)

    # Process duplicates in parallel
    batch_size = 10000
    with tqdm(total=total_duplicates, desc="Processing duplicates") as pbar:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for i in range(0, total_duplicates, batch_size):
                batch = duplicates[i:i + batch_size]
                futures.append(executor.submit(process_batch, batch, True))

            for future in as_completed(futures):
                pbar.update(batch_size)

    # Identify non-duplicates pipeline
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

    # Get and process non-duplicates
    non_duplicates = list(tweets_collection.aggregate(non_duplicates_pipeline))
    total_non_duplicates = len(non_duplicates)

    # Process non-duplicates in parallel
    with tqdm(total=total_non_duplicates, desc="Processing non-duplicates") as pbar:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for i in range(0, total_non_duplicates, batch_size):
                batch = non_duplicates[i:i + batch_size]
                futures.append(executor.submit(process_batch, batch, False))

            for future in as_completed(futures):
                pbar.update(batch_size)

# Call the function to execute the code
remove_duplicates()





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



