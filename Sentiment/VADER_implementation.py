from statistics import mean
from turtle import pos
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from bson.objectid import ObjectId
import nltk

def update_VADER(analyzer: SentimentIntensityAnalyzer) -> SentimentIntensityAnalyzer:
    """
    Takes a VADER analyzer and updates it with custom words with custom values.
    :param analyzer: the VADER analyzer that will be changed
    :returns: the updated analyzer
    """

    analyzer.lexicon['help'] = 0
    analyzer.lexicon['cancellation'] = -2.29
    analyzer.lexicon['cancelled'] = analyzer.lexicon['cancellation']
    analyzer.lexicon['canceled'] = analyzer.lexicon['cancellation']
    analyzer.lexicon['cancels'] = analyzer.lexicon['cancellation']
    analyzer.lexicon['cancelation'] = analyzer.lexicon['cancellation']
    analyzer.lexicon['cancelations'] = analyzer.lexicon['cancellation']
    analyzer.lexicon['cancellations'] = analyzer.lexicon['cancellation']
    analyzer.lexicon['long'] = -1.06
    analyzer.lexicon['transfer'] = 0.13
    analyzer.lexicon['transferred'] = analyzer.lexicon['transfer']
    analyzer.lexicon['nonstop'] = 0.06
    analyzer.lexicon['non-stop'] = analyzer.lexicon['nonstop']
    analyzer.lexicon['non stop'] = analyzer.lexicon['non-stop']
    analyzer.lexicon['direct'] = 0.45
    analyzer.lexicon['directly'] = analyzer.lexicon['direct']
    analyzer.lexicon['over-booked'] = -1.91
    analyzer.lexicon['overbooked'] = analyzer.lexicon['over-booked']
    analyzer.lexicon['over booked'] = analyzer.lexicon['over-booked']
    analyzer.lexicon['over books'] = analyzer.lexicon['over-booked']
    analyzer.lexicon['overbooks'] = analyzer.lexicon['over-booked']
    analyzer.lexicon['over-books'] = analyzer.lexicon['over-booked']
    analyzer.lexicon['offload'] = -1.63
    analyzer.lexicon['offloads'] = analyzer.lexicon['offload']
    analyzer.lexicon['offloaded'] = analyzer.lexicon['offload']
    analyzer.lexicon['off-load'] = analyzer.lexicon['offload']
    analyzer.lexicon['off-loaded'] = analyzer.lexicon['offload']
    analyzer.lexicon['off-loads'] = analyzer.lexicon['offload']
    analyzer.lexicon['problem'] = -2.24
    analyzer.lexicon['problems'] = analyzer.lexicon['problem']
    analyzer.lexicon['on time'] = 1.69
    analyzer.lexicon['ontime'] = analyzer.lexicon['on time']
    analyzer.lexicon['on-time'] = analyzer.lexicon['on time']
    analyzer.lexicon['terminal'] = 0.03

    return analyzer

def analyze_sentiment(text: str) -> dict[str, float]:
    """
    Analyzes the sentiment of a piece of text and returns the sentiment scores.
    :param text: the string that contains the text that will be analyzed
    """

    sentiment_score = analyzer.polarity_scores(text)
    return sentiment_score

def get_full_text(tweet) -> str:
    """
    Gets the full text of a tweet object. Either just the text field or the full_text field if it has been truncated.
    :param tweet: the tweet to get the text from
    """

    if tweet.get('truncated', True):
        return tweet.get('extended_tweet', {}).get('full_text', '')
    else:
        return tweet.get('text', '')
    


def add_entire_document(document, new_collection) -> None:
        """
        Inserts a document into a MongoDB collection.
        :param document: document to be inserted
        :param new_collection: the collection the document will be inserted into
        """

        new_collection.insert_one(document)



def add_sentiment_variables(database, old_collection, new_collection_name: str) -> None:
    """
    Creates a new collection and fills it with the documents from the old collection 
    and adds the sentiment variables to all documents in the new collection.
    :param database: the database that should hold the new collection
    :param old_collection: the collection of tweets that will be copied and analyzed
    :param new_collection_name: the name of the new collection that will include the sentiment variables.
    """

    # Process documents in batches
    batch_size = 10000
    documents_processed = 0

    # Create the new collection if it does not yet exist
    if new_collection_name not in database.list_collection_names():
        new_collection = database.create_collection(new_collection_name)
    else:
        new_collection = database[new_collection_name]


    while True:
        # Retrieve a batch of documents
        batch = list(old_collection.find({}).skip(documents_processed).limit(batch_size))
        if not batch:
            print('Sentiment Analysis Finished!')
            break  # Exit loop if no more documents are retrieved
        
        # Analyze sentiment for each document in the batch
        for document in batch:
            text = get_full_text(document)
            sentiment = analyze_sentiment(text)

            # Copy the tweet into the new collection
            add_entire_document(document, new_collection)

            # Add the sentiment variables to the tweet in the new collection
            if text[-1:] == '…':
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

# Get the VADER analyzer
nltk.download('vader_lexicon')
analyzer = SentimentIntensityAnalyzer()
analyzer = update_VADER(analyzer)

