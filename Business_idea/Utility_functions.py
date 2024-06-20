import pymongo
import re
import gc
from bertopic import BERTopic
from pymongo import MongoClient


client = MongoClient("mongodb://localhost:27017/")
db = client.DBL

def get_full_text(tweet):
    if tweet.get('truncated', True):
        return tweet.get('extended_tweet', {}).get('full_text', '')

    else:
        return tweet.get('text', '')
    

def batch_generator(collection, batch_size):
    tweets_cursor = collection.find({}).limit(batch_size)
    batch = list(get_full_text(tweet) for tweet in tweets_cursor)
    yield [clean(tweet_text) for tweet_text in batch]
    del batch
    gc.collect()




def clean(text):
    #remove @ppl, url
    output = re.sub(r'https://\S*','', text)
    output = re.sub(r'@\S*','',output)
    
    #remove \r, \n
    rep = r'|'.join((r'\r',r'\n'))
    output = re.sub(rep,'',output)

      #remove duplicated punctuation
    output = re.sub(r'([!()\-{};:,<>./?@#$%\^&*_~]){2,}', lambda x: x.group()[0], output)
    
    #remove extra space
    output = re.sub(r'\s+', ' ', output).strip()
    
    #remove string if string only contains punctuation
    if sum([i.isalpha() for i in output])== 0:
        output = ''
        
    #remove string if length<5
    if len(output.split()) < 5:
        output = ''

    output = output.lower()

    return output


def get_random_tweet(batch_size):
    #connect
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client.Airline_data
    collection = db.topic_analysis

    #pipeline 
    pipeline = [
        {
                '$sample': { 'size': batch_size }  # Get random document
        }
       
    ]

    return list(collection.aggregate(pipeline))


def add_entire_document(document, new_collection):
        new_collection.insert_one(document)

def add_to_document(doc_id, new_field: str, new_value, new_collection):
    collection = new_collection
    if collection.find_one({'_id' : doc_id}) != None:
        collection.update_one(
        {"_id": ObjectId(str(doc_id))},
        {"$set": {new_field: new_value}}
        )

def get_real_label(label: str, topic_labels: list):
    
    return topic_labels[(topic_labels.index(label) + 1) % len(topic_labels)] 


def filter_tweets_by_mention(collection, user_handle):
    mentions = []
    for tweet in collection.find():
        if user_handle in tweet.get('text', ''):
            mentions.append(tweet)
    return mentions

# AmericanAir_tweets = db['AmericanAir_tweets']
# for tweet in filter_tweets_by_mention(db['Sentiment_included'], 'AmericanAir'):
#     AmericanAir_tweets.insert_one(tweet)



def new_labels(label, topic_labels):
    label = get_real_label(label, topic_labels)
    index = topic_labels.index(label)
    new_labels = ['Undefined','Response', 'Qantas', 'Posted', 'Ryanair', 'Baggage', 'Easyjet', 'Avgeek', 'Lufthansa', 'Seats', 'Delays', 'Racial Retweet', 'Coronavirus','Cancelled','Booking reference', 'Airline','Refunds','KLM','Thank you for Flight','Roundtrip','Retweet backless seats', 'Twitter jargon','Retweet pride parade','Food','Retweet Breastfeeding','Retweet Lesbian','Climate','Retweet Lufthansa Iran','General Retweet','Corporate affairs','Racism','Locating baggage','Racial Retweet','Refunds','Retweet muslims','Italy','Retweet non-airline incident','Places in england','Retweet BTS','Flying','Retweet discrimination corona','Retweet lesbian sexualization','Flight tracker','Workers and Catering','Claims','Retweet Nigerian lawyer','Retweet Terrorism','Europa AirFrance']

    return new_labels[index]