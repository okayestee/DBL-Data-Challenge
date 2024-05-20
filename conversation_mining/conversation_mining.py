#setup mongoDB connection
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client.DBL2 #your database
tweets_collection = db.clean_finalversion #your collection within database
convo_airline_related = db.conversation1 #collection that will contain airline related convos

airline_keywords = ['airline', 'flight', 'airport', 'airplane', 'plane', 'boarding', 'departure', 'arrival']
airline_tags = ['@AmericanAir']

#create indexes on relevant fields, speeds up the matching process
tweets_collection.create_index([('text', 'text')])
tweets_collection.create_index([('entities.user_mentions.screen_name', 1)])
tweets_collection.create_index([('media', 1)])
tweets_collection.create_index([('links', 1)])

pipeline = [
    #Filter tweets based on keywords and airline mentions
    {
        '$match' : {
            '$or' : [
                #match tweets with keywords in text
                { 'text': { '$regex': '|'.join(airline_keywords), '$options': 'i' } },
                #match tweets mentioning airline tags
                    { 'entities.user_mentions.screen_name': { '$in': [tag[1:] for tag in airline_tags] } }
            ],
        }
    }
]

filtered_tweets = list(tweets_collection.aggregate(pipeline))

#for tweet in filtered_tweets:
    #print(tweet)
