from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['AirplaneMode']
tweets_collection = db['tweets']

# Function to filter tweets that mention a specific @user and return a list of those tweets
def filter_tweets_by_mention(collection, user_handle):
    mentions = []
    for tweet in collection.find():
        if user_handle in tweet.get('text', ''):
            mentions.append(tweet)
    return mentions

def main():
    user_handle = '@americanair'  # Change this to any handle you want to filter by
    mentions = filter_tweets_by_mention(tweets_collection, user_handle)
    
    # Print the filtered tweets
    for mention in mentions:
        print(f"- {mention}")
    
    return mentions

if __name__ == "__main__":
    mentions = main()

client.close()



