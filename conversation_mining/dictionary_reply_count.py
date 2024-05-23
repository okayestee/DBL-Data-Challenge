from pymongo import MongoClient

#fetching tweets

def fetch_tweet_ids(collection): 
    '''
    retrieves all tweet IDs from MongoDB
    ''' 
    cursor = collection.find({}, {'id': 1}) 
    tweet_ids = [document['id']for document in cursor]
    return tweet_ids

def insert_removed_duplicates(tweet_ids, collection): 
    '''
    insert removed duplicates into another MongoDB collection
    '''
    removed_duplicates = [{'id': tweet_id, 'reply_count': 0} for tweet_id in tweet_ids]
    collection.insert_many(removed_duplicates)

def increment_reply_count(collection, tweet_id, increment_by=1): 
    '''
    increment reply count for a given tweet_id
    '''
    collection.update_one({'id': tweet_id}, {'$inc': {'reply_count': increment_by}})

if __name__ == '__main__': 
    client = MongoClient('mongodb://localhost:27017/')

    db = client['DBL'] # your database
    tweets_collection = db['removed_duplicates'] #existing collection
    
    tweet_ids = fetch_tweet_ids(tweets_collection)

    dict_count_replies = db['count_replies']
    insert_removed_duplicates(tweet_ids, dict_count_replies)

    # #practice test
    # for doc in dict_count_replies.find().limit(50):
    #     print(doc)

