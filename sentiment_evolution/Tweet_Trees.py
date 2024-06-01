from pymongo import MongoClient, InsertOne
from treelib import Tree
from tqdm import tqdm

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client.AirplaneMode

# Collections
no_single_tweets_collection = db.no_single_tweets
tweet_trees_collection = db.Tweet_Trees

# Ensure the Tweet_Trees collection is empty
tweet_trees_collection.drop()

# Function to build tree and store in collection
def build_tree_batch(documents):
    tree = Tree()

    for doc in documents:
        tweet_id = doc['id_str']
        parent_id = doc.get('in_reply_to_status_id_str')

        if parent_id:
            try:
                tree.create_node(tweet_id, tweet_id, parent=parent_id, data=doc)
            except treelib.exceptions.NodeIDAbsentError:
                continue  # Skip if parent node does not exist
        else:
            tree.create_node(tweet_id, tweet_id, data=doc)

    return tree

# Function to fetch batch of documents
def fetch_batch(collection, query, projection, skip, limit):
    return list(collection.find(query, projection).skip(skip).limit(limit))

# Function to build tree and store in collection
def build_tree_batch_and_store(batch_size, total_docs):
    for skip in range(0, total_docs, batch_size):
        batch_documents = fetch_batch(no_single_tweets_collection, {}, {'_id': 0}, skip, batch_size)
        if not batch_documents:
            break

        tweet_tree = build_tree_batch(batch_documents)
        if tweet_tree.size() > 0:
            tweet_trees_collection.insert_one({'tree': tweet_tree.to_dict()})

# Main function to build trees and store in collection
def main():
    batch_size = 10000

    # Get total count of documents in no_single_tweets collection
    total_docs = no_single_tweets_collection.count_documents({})

    # Build trees and store in collection
    with tqdm(total=total_docs, desc="Building trees and storing in collection") as pbar:
        build_tree_batch_and_store(batch_size, total_docs)
        pbar.update(total_docs)

if __name__ == "__main__":
    main()

# Close the connection
client.close()
