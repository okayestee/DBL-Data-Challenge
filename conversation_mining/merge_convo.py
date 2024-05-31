import json
import sys
from tqdm import tqdm
from pymongo import MongoClient
from bson.objectid import ObjectId

def merge_consecutive_tweets(data):
    merged_tweets = []
    current_user_id = None
    first_tweet_id = None

    for tweet_data in data:
        tweet_id = tweet_data["id_str"]  # Get tweet ID
        user_id = tweet_data["user"]["id"]

        if user_id != current_user_id:
            if merged_tweets:  # If previous tweet was merged
                merged_tweets[0]["child_ids"] = [tweet_id]  # Update child_ids of first tweet
                merged_tweets[0]["is_merged"] = True  # Mark as merged
                merged_tweets[0]["merged_text"] += " " + tweet_data["text"]  # Merge text
                if "extended_tweet" in tweet_data:  # Merge extended tweet
                    if "extended_tweet" in merged_tweets[0]:
                        merged_tweets[0]["extended_tweet"]["full_text"] += " " + tweet_data["extended_tweet"]["full_text"]
                    else:
                        merged_tweets[0]["extended_tweet"] = tweet_data["extended_tweet"]
                continue

            tweet_data["merged_text"] = tweet_data["text"]  # Initialize merged_text field
            merged_tweets.append(tweet_data)
            first_tweet_id = tweet_id
            current_user_id = user_id
        else:
            merged_tweets[0]["merged_text"] += " " + tweet_data["text"]
            if "extended_tweet" in tweet_data:
                if "extended_tweet" in merged_tweets[0]:
                    merged_tweets[0]["extended_tweet"]["full_text"] += " " + tweet_data["extended_tweet"]["full_text"]
                else:
                    merged_tweets[0]["extended_tweet"] = tweet_data["extended_tweet"]

    return merged_tweets

def process_tweets(db):
    cursor = db.airline_trees.find({}, no_cursor_timeout=True)

    for document in cursor:
        tree_id = str(document["_id"])
        original_trees = document["tree_data"]

        for tree_key in original_trees:
            tree = original_trees[tree_key]
            children = tree["children"]

            total_tweets = len(children)
            batch_size = 100
            batches = [children[i:i+batch_size] for i in range(0, total_tweets, batch_size)]

            merged_children = []

            for batch in tqdm(batches, desc=f"Processing Tree {tree_key}", file=sys.stdout):
                merged_children.extend(merge_consecutive_tweets(batch))

            tree["children"] = merged_children

        db.merged_trees.insert_one({"_id": ObjectId(), "tree_data": original_trees})

    cursor.close()

if __name__ == "__main__":
    client = MongoClient("mongodb://localhost:27017/")
    db = client.DBL2
    process_tweets(db)


#to check 
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client.DBL2  # Replace "new_database" with your new database name
collection = db.airline_trees # Replace "new_collection" with your new collection name

# Query documents from the collection
cursor = collection.find({})

# Flag to track if consecutive replies are found
consecutive_replies_found = False

# Iterate over the documents
for document in cursor:
    # Iterate over trees in the document
    for tree_key, tree_data in document["tree_data"].items():
        children = tree_data["children"]
        
        # Iterate over children in the tree
        for i in range(len(children) - 1):
            current_tweet = children[i]
            next_tweet = children[i + 1]
            
            # Check if consecutive tweets are from the same user
            if current_tweet["user"]["id"] == next_tweet["user"]["id"]:
                # Consecutive replies found
                consecutive_replies_found = True
                print(f"Consecutive replies found in Tree {tree_key}:")
                print(f"Tweet 1: ID: {current_tweet['id_str']}, Text: {current_tweet['text']}")
                print(f"Tweet 2: ID: {next_tweet['id_str']}, Text: {next_tweet['text']}")
                print()
                break  # Break the loop if consecutive replies found
        
        # Break the outer loop if consecutive replies found
        if consecutive_replies_found:
            break
    
    # Break the outer loop if consecutive replies found
    if consecutive_replies_found:
        break

# Close the cursor
cursor.close()

