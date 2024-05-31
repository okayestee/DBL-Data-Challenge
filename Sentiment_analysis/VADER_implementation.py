from collections.abc import Collection
from turtle import pos
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import pymongo
from bson.objectid import ObjectId



# Connect to the database
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['DBL_data']
collection = db['no_inconsistency']


# Initialize VADER Sentiment Analyzer
analyzer = SentimentIntensityAnalyzer()

# Function to run VADER analysis on text
def analyze_sentiment(text):
    sentiment_score = analyzer.polarity_scores(text)
    return sentiment_score

def get_full_text(tweet):
    if tweet.get('truncated', True):
        return tweet.get('extended_tweet', {}).get('full_text', '')
    else:
        return tweet.get('text', '')
    
def add_to_document(doc_id, new_field: str, new_value, new_collection):
    collection = new_collection
    if collection.find_one({'_id' : doc_id}) != None:
        collection.update_one(
        {"_id": ObjectId(str(doc_id))},
        {"$set": {new_field: new_value}}
        )

def add_entire_document(document, new_collection):
        new_collection.insert_one(document)

## Process documents in batches
batch_size = 10000
documents_processed = 0

weird_counter = 0

if 'sentiment_analysis' not in db.list_collection_names():
    new_collection = db.create_collection("sentiment_analysis")
else:
    new_collection = db['sentiment_analysis']


while True:
    # Retrieve a batch of documents
    batch = list(collection.find({}).skip(documents_processed).limit(batch_size))
    if not batch:
        print('Sentiment Analysis Finished!')
        break  # Exit loop if no more documents are retrieved
    
    # Analyze sentiment for each document in the batch
    for document in batch:
        text = get_full_text(document)
        sentiment = analyze_sentiment(text)

        add_entire_document(document, new_collection)

        if text[-1:] == '…':
            weird_counter += 1
            new_collection.update_one(
            {"_id": ObjectId(document.get('_id'))},
            {"$set": {'negativity': sentiment['neg'], 'neutrality': sentiment['neu'], 'positivity' : sentiment['pos'], 'compound sentiment' : sentiment['compound'], 'truncated_error' : True}}
            )
        else:
            new_collection.update_one(
            {"_id": ObjectId(document.get('_id'))},
            {"$set": {'negativity': sentiment['neg'], 'neutrality': sentiment['neu'], 'positivity' : sentiment['pos'], 'compound sentiment' : sentiment['compound'], 'truncated_error' : False}}
            )    

    # Update the count of processed documents
    documents_processed += len(batch)
    print(documents_processed)