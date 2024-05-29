import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import pymongo

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
collection = db['removed_duplicates']


# Initialize VADER Sentiment Analyzer
analyzer = SentimentIntensityAnalyzer()

# Function to run VADER analysis on text
def analyze_sentiment(text):
    sentiment_score = analyzer.polarity_scores(text)
    return sentiment_score

def get_full_text(tweet):
    if tweet.get('truncated', False):
        print('Got Full Text')
        return tweet.get('extended_tweet', {}).get('full_text', '')

    else:
        return tweet.get('text', '')

## Process documents in batches
batch_size = 1000
documents_processed = 0

while True:
    # Retrieve a batch of documents
    batch = list(collection.find({}).skip(documents_processed).limit(batch_size))
    if not batch:
        break  # Exit loop if no more documents are retrieved
    
    # Analyze sentiment for each document in the batch
    for document in batch:
        text = get_full_text(document)
        sentiment = analyze_sentiment(text)
        print(f"Text: {text}")
        print(f"Sentiment: {sentiment}")
    
    # Update the count of processed documents
    documents_processed += len(batch)