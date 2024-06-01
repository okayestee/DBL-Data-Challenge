from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['DBL2']
tweets_collection = db['user_roots2']

# Function to filter tweets that mention a specific @user and return a list of those tweets
def filter_tweets_by_mention(collection, user_handle):
    mentions = []
    for tweet in collection.find():
        if user_handle in tweet.get('text', ''):
            mentions.append(tweet)
    return mentions

def main():
    user_handle = '@americanair'  # Change this to the user you want to filter on
    mentions = filter_tweets_by_mention(tweets_collection, user_handle)
    
    # Print the filtered tweets
    for mention in mentions:
        print(f"- {mention['text']}")
    
    return mentions

if __name__ == "__main__":
    mentions = main()

client.close()


