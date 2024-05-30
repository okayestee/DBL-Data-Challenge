from collections.abc import Collection
from turtle import pos
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import pymongo
from bson.objectid import ObjectId

# analyzer = SentimentIntensityAnalyzer()

# text = 'Christmas ruined'

# with open('Sentiment analysis/sample', 'r', encoding='utf-8') as file:
#     texts: list[str] = file.readlines()

# for tweet in texts:
#     scores = analyzer.polarity_scores(tweet)
#     print(tweet)
#     print(f'{scores}\n')

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
        text = tweet.get('text', '')
        return tweet.get('text', '')
    
def add_to_collection(doc_id, new_field: str, new_value, new_collection):
    collection = new_collection
    if collection.find_one({'_id' : doc_id}) != None:
        collection.update_one(
        {"_id": ObjectId(str(doc_id))},
        {"$set": {new_field: new_value}}
        )
    else:
        collection.insert_one(
        {"_id": ObjectId(str(doc_id))},
        {"$set": {new_field: new_value}}
        )

def add_entire_document(doc_id, new_collection):
    collection.insert_one(
        {"_id": ObjectId(str(doc_id))}
    )

## Process documents in batches
batch_size = 10000
documents_processed = 0

weird_counter = 0

new_collection = db.create_collection("sentiment_analysis")

new_collection.update_one

while True:
    # Retrieve a batch of documents
    batch = list(collection.find({}).skip(documents_processed).limit(batch_size))
    if not batch:
        break  # Exit loop if no more documents are retrieved
    
    # Analyze sentiment for each document in the batch
    for document in batch:
        text = get_full_text(document)
        sentiment = analyze_sentiment(text)

        negativity = sentiment['neg']
        neutrality = sentiment['neu']
        positivity = sentiment['pos']
        compound_score = sentiment['compound']

        add_entire_document(document.get('_id'), new_collection)
        add_to_collection(document.get('_id'), 'negativity', negativity, new_collection)
        add_to_collection(document.get('_id'), 'neutrality', neutrality, new_collection)
        add_to_collection(document.get('_id'), 'positivity', positivity, new_collection)
        add_to_collection(document.get('_id'), 'Compound sentiment', compound_score, new_collection)

        if text[-1:] == '…':
            weird_counter += 1
            add_to_collection(document.get('_id'), 'truncated_error', True, new_collection)
        else:
            add_to_collection(document.get('_id'), 'truncated_error', False, new_collection)
            
        print(f"Text: {text}")
        print(f"Sentiment: {sentiment}")
    
    # Update the count of processed documents
    documents_processed += len(batch)
    print(documents_processed)

    print(weird_counter)