from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['DBL2']  # Replace with your database name
old_collection = db['airline_trees']  # Replace with your old collection name
new_collection = db['merge_trees']  # Replace with your new collection name

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


