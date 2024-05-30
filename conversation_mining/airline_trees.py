from pymongo import MongoClient, IndexModel, ASCENDING
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import json

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client.AirplaneMode

# Collections
replies_collection = db.replies
convo_starters_collection = db.airline_convo_starters

# Ensure indexes are created
convo_starters_collection.create_indexes([IndexModel([('id_str', ASCENDING)])])
replies_collection.create_indexes([IndexModel([('in_reply_to_status_id_str', ASCENDING)])])

# Function to fetch batch of documents
def fetch_batch(collection, query, projection, skip, limit):
    return list(collection.find(query, projection).skip(skip).limit(limit))

# Function to process replies in batches and build initial tweet dictionary
def process_replies_batch(batch):
    tweet_dict = {}
    parent_ids = set()
    for reply in batch:
        tweet_dict[reply['id_str']] = {'tweet': reply, 'children': []}
        parent_ids.add(reply['in_reply_to_status_id_str'])
    return tweet_dict, parent_ids

# Generator for processing replies in batches
def process_replies(batch_size, total_replies):
    for skip in range(0, total_replies, batch_size):
        batch = fetch_batch(replies_collection, {}, {'_id': 0}, skip, batch_size)
        if not batch:
            break
        yield from batch

# Function to add nodes to a tree
def add_node(tweet_id, tweet_dict, tree):
    if tweet_id not in tweet_dict:
        return None

    tweet_info = tweet_dict[tweet_id]
    tweet = tweet_info['tweet']
    parent_id = tweet.get('in_reply_to_status_id_str')

    # If this tweet is a reply to another tweet
    if parent_id and parent_id in tweet_dict:
        parent_node = add_node(parent_id, tweet_dict, tree)
        if parent_node:
            parent_node['children'].append(tweet_info)
        else:
            return None
    else:
        # This is a root tweet
        tree[tweet_id] = tweet_info

    return tweet_info

# Function to process tweets in parallel
def process_tweet_batch(batch, tweet_dict):
    local_trees = {}
    for tweet_id in batch:
        add_node(tweet_id, tweet_dict, local_trees)
    return local_trees

# Function to build trees from replies
def build_trees(batch_size, total_replies):
    tweet_dict = {}
    parent_ids = set()

    with tqdm(total=total_replies, desc="Processing replies") as pbar:
        for reply in process_replies(batch_size, total_replies):
            tweet_dict[reply['id_str']] = {'tweet': reply, 'children': []}
            parent_ids.add(reply['in_reply_to_status_id_str'])
            pbar.update(1)
    
    all_tweet_ids = list(tweet_dict.keys())
    trees = {}
    with tqdm(total=len(all_tweet_ids), desc="Building trees") as pbar:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for i in range(0, len(all_tweet_ids), batch_size):
                batch = all_tweet_ids[i:i + batch_size]
                futures.append(executor.submit(process_tweet_batch, batch, tweet_dict))

            for future in as_completed(futures):
                result = future.result()
                trees.update(result)
                pbar.update(batch_size)

    return trees

# Function to match trees with conversation starters and make matched tweets new root nodes
def match_trees_with_convo_starters(trees):
    matched_trees = {}
    parent_to_children = {}

    # Get the id_str values of conversation starters
    convo_starters_ids = set(doc['id_str'] for doc in convo_starters_collection.find({}, {'_id': 0, 'id_str': 1}))

    with tqdm(total=len(trees), desc="Matching trees with convo starters") as pbar:
        for root_id, root_node in trees.items():
            root_tweet = root_node['tweet']
            parent_id = root_tweet.get('in_reply_to_status_id_str')
            if parent_id in convo_starters_ids:
                if parent_id not in parent_to_children:
                    parent_to_children[parent_id] = []
                parent_to_children[parent_id].append(root_node)
            pbar.update(1)

    # Create matched trees with convo starters as new roots
    for parent_id, children in parent_to_children.items():
        matched_tweet = convo_starters_collection.find_one({'id_str': parent_id})
        if matched_tweet:
            matched_trees[parent_id] = {'tweet': matched_tweet, 'children': []}
            for child in children:
                matched_trees[parent_id]['children'].extend(child['children'])

    return matched_trees

# Function to save matched trees to a JSON file with progress visualization
def save_matched_trees_to_json(matched_trees, filename):
    with tqdm(total=1, desc="Saving matched trees") as pbar:  # Only one JSON dump operation
        with open(filename, 'w') as file:
            json.dump(matched_trees, file, indent=2)
            pbar.update(1)

# Main function to build trees and save matched trees to JSON file
def main():
    batch_size = 10000

    # Get total count of replies
    total_replies = replies_collection.count_documents({})

    # Build trees from replies
    trees = build_trees(batch_size, total_replies)

    # Match trees with conversation starters and make matched tweets new root nodes
    matched_trees = match_trees_with_convo_starters(trees)

    # Save matched trees to a JSON file
    save_matched_trees_to_json(matched_trees, '/Users/20235050/Downloads/processed_data/airline_matched_trees.json')

if __name__ == "__main__":
    main()
