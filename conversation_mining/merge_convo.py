'''
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['DBL_test']  # Replace with your database name
old_collection = db['airline_test']  # Replace with your old collection name
new_collection = db['merge_trees_test1']  # Replace with your new collection name

def merge_consecutive_tweets(document):
    def merge_tweets(tweets):
        merged_tweets = []
        i = 0
        while i < len(tweets):
            current_tweet = tweets[i]
            current_user = current_tweet['user']['id_str']
            j = i + 1
            
            while j < len(tweets) and tweets[j]['user']['id_str'] == current_user:
                # Combine text fields
                current_tweet['text'] += ' ' + tweets[j]['text']
                # Combine extended_tweet fields if present
                if 'extended_tweet' in current_tweet and 'extended_tweet' in tweets[j]:
                    current_tweet['extended_tweet']['full_text'] += ' ' + tweets[j]['extended_tweet']['full_text']
                elif 'extended_tweet' in tweets[j]:
                    if 'extended_tweet' in current_tweet:
                        current_tweet['extended_tweet']['full_text'] += ' ' + tweets[j]['extended_tweet']['full_text']
                    else:
                        current_tweet['extended_tweet'] = tweets[j]['extended_tweet']
                j += 1
                
            merged_tweets.append(current_tweet)
            i = j
        
        return merged_tweets

    def update_in_reply_to_status_id(tweets):
        for index in range(1, len(tweets)):
            tweets[index]['in_reply_to_status_id'] = tweets[index-1]['id']
            tweets[index]['in_reply_to_status_id_str'] = tweets[index-1]['id_str']

    def delete_second_reply(tweets):
        if len(tweets) > 1:
            del tweets[1]

    def traverse_and_merge(node):
        if 'children' not in node:
            return

        # Merge consecutive tweets in the current node's children
        for child_id in node['children']:
            node['children'][child_id] = merge_tweets(node['children'][child_id])
            # Update in_reply_to_status_id for merged tweets
            update_in_reply_to_status_id(node['children'][child_id])
            # Delete the second reply of merged tweets
            delete_second_reply(node['children'][child_id])

            # Update in_reply_to_status_id for children of merged tweets
            for tweet in node['children'][child_id]:
                if 'children' in tweet:
                    traverse_and_merge(tweet)

        # Recursively apply to all children
        for child_id in node['children']:
            traverse_and_merge(node['children'][child_id])

    # Start merging from the root document
    traverse_and_merge(document)

# Process documents and insert into new collection
for document in old_collection.find():
    merge_consecutive_tweets(document.copy())
    new_collection.insert_one(document)
'''


from pymongo import MongoClient

def merge_consecutive_tweets(tree_data):
    merged_tweets = {}  # Dictionary to store merged tweets

    current_user_id = None
    current_user_id_str = None
    current_tweet_id = None
    merged_tweet_text = ""

    # Iterate over the tweets in the tree data
    for tweet_key, tweet_info in tree_data.items():
        tweet_data = tweet_info['data']
        user_id = str(tweet_data['user']['id'])  # Convert user ID to string
        user_id_str = tweet_data['user']['id_str']
        tweet_id = tweet_data['id_str']
        text = tweet_data['text']

        # Check if this tweet has the same user ID and user ID string as the previous one
        if user_id == current_user_id and user_id_str == current_user_id_str:
            # Append the text of this tweet to the merged text
            merged_tweet_text += " " + text

            # Update the tweet ID to the latest one
            current_tweet_id = tweet_id
        else:
            # If merged tweet text is not empty, create a merged tweet
            if merged_tweet_text:
                merged_tweets[current_user_id] = {
                    'id_str': current_tweet_id,
                    'text': merged_tweet_text.strip()
                }

            # Reset merged tweet text and current tweet ID for the new user ID
            merged_tweet_text = text
            current_user_id = user_id
            current_user_id_str = user_id_str
            current_tweet_id = tweet_id

    # Handle the last merged tweet if any
    if merged_tweet_text:
        merged_tweets[current_user_id] = {
            'id_str': current_tweet_id,
            'text': merged_tweet_text.strip()
        }

    return merged_tweets

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['DBL_test']
collection = db['airline_test']

# Fetch the first document from the collection
document = collection.find_one({})

# Check if a document is found
if document:
    # Extract tree data from the document
    tree_data = document.get('tree_data', {})

    # Merge consecutive tweets with the same user ID and user ID string
    merged_tweets = merge_consecutive_tweets(tree_data)

    # Convert the dictionary keys to strings
    merged_tweets_str_keys = {str(k): v for k, v in merged_tweets.items()}

    # Print the merged tweets
    for user_id, tweet_info in merged_tweets_str_keys.items():
        print("User ID:", user_id)
        print("Merged Tweet Text:", tweet_info['text'])

        # Print the full merged tweet
        print("Full Merged Tweet:", tweet_info)

    # Store the merged tweets in a new collection in MongoDB
    merged_collection = db['merged_tweets']
    merged_collection.insert_one(merged_tweets_str_keys)
else:
    print("No document found in the collection.")








