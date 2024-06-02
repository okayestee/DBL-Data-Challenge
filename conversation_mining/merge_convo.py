# from pymongo import MongoClient

# def merge_consecutive_tweets(tree_data):
#     merged_tweets = {}  # Dictionary to store merged tweets
#     merged_tweet_ids = set()  # Set to store IDs of merged tweets

#     # Iterate over the root nodes in the tree data
#     for root_id, root_info in tree_data.items():
#         # Check if the node has children
#         if 'children' in root_info:
#             # Traverse each subtree and check for consecutive replies
#             for child_info in root_info['children']:
#                 merge_subtree_consecutive_tweets(child_info, merged_tweets, merged_tweet_ids)

#     return merged_tweets, merged_tweet_ids

# def merge_subtree_consecutive_tweets(subtree_root_info, merged_tweets, merged_tweet_ids):
#     current_user_id = None
#     current_user_id_str = None
#     current_tweet_id = None
#     merged_tweet_text = ""

#     # Traverse the subtree
#     for child_id, child_info in subtree_root_info.items():
#         # Check if the child has 'data' field
#         if 'data' in child_info:
#             tweet_data = child_info['data']
#             user_id = str(tweet_data['user']['id'])  # Convert user ID to string
#             user_id_str = tweet_data['user']['id_str']
#             tweet_id = tweet_data['id_str']
#             text = tweet_data['text']

#             # Check if this tweet has the same user ID and user ID string as the previous one
#             if user_id == current_user_id and user_id_str == current_user_id_str:
#                 # Append the text of this tweet to the merged text
#                 merged_tweet_text += " " + text
#             else:
#                 # If merged tweet text is not empty, create a merged tweet
#                 if merged_tweet_text:
#                     merged_tweets[current_user_id] = {
#                         'id_str': current_tweet_id,
#                         'text': merged_tweet_text.strip()
#                     }
#                     merged_tweet_ids.add(current_tweet_id)

#                 # Reset merged tweet text and current tweet ID for the new user ID
#                 merged_tweet_text = text
#                 current_user_id = user_id
#                 current_user_id_str = user_id_str
#                 current_tweet_id = tweet_id

#     # Handle the last merged tweet if any
#     if merged_tweet_text:
#         merged_tweets[current_user_id] = {
#             'id_str': current_tweet_id,
#             'text': merged_tweet_text.strip()
#         }
#         merged_tweet_ids.add(current_tweet_id)

# # Connect to MongoDB
# client = MongoClient('mongodb://localhost:27017/')
# db = client['AirplaneMode']
# collection = db['airline_trees']

# # Fetch the documents from the collection
# documents = collection.find({})

# # Check if documents are found
# if documents:
#     # Initialize list to store merged trees
#     merged_trees = []

#     # Iterate over the documents
#     for document in documents:
#         # Extract tree data from the document
#         tree_data = document.get('tree_data', {})

#         # Merge consecutive tweets with the same user ID and user ID string
#         merged_tweets, merged_tweet_ids = merge_consecutive_tweets(tree_data)

#         # Add a field 'merged' to the merged tweets
#         for user_id, tweet_info in merged_tweets.items():
#             tweet_info['merged'] = True

#         # Add the merged tree to the list
#         merged_trees.append(tree_data)

#     # Store the merged trees in the new collection in MongoDB
#     merged_collection = db['merged_trees']
#     merged_collection.insert_many(merged_trees)

# else:
#     print("No documents found in the collection.")

from pymongo import MongoClient

def merge_consecutive_tweets(tree_data):
    merged_tweets = {}  # Dictionary to store merged tweets
    merged_tweet_ids = set()  # Set to store IDs of merged tweets

    # Iterate over the root nodes in the tree data
    for root_id, root_info in tree_data.items():
        # Check if the node has children
        if 'children' in root_info:
            # Traverse each subtree and check for consecutive replies
            for child_info in root_info['children']:
                merge_subtree_consecutive_tweets(child_info, merged_tweets, merged_tweet_ids)

    return merged_tweets, merged_tweet_ids

def merge_subtree_consecutive_tweets(subtree_root_info, merged_tweets, merged_tweet_ids):
    current_user_id = None
    current_user_id_str = None
    current_tweet_id = None
    merged_tweet_text = ""

    # Traverse the subtree
    for child_id, child_info in subtree_root_info.items():
        # Check if the child has 'data' field
        if 'data' in child_info:
            tweet_data = child_info['data']
            user_id = str(tweet_data['user']['id'])  # Convert user ID to string
            user_id_str = tweet_data['user']['id_str']
            tweet_id = tweet_data['id_str']
            text = tweet_data['text']

            # Check if this tweet has the same user ID and user ID string as the previous one
            if user_id == current_user_id and user_id_str == current_user_id_str:
                # Append the text of this tweet to the merged text
                merged_tweet_text += " " + text
            else:
                # If merged tweet text is not empty, mark the previous tweet as merged
                if merged_tweet_text:
                    merged_tweets[current_tweet_id]['merged'] = True
                    merged_tweet_ids.add(current_tweet_id)

                # Reset merged tweet text and current tweet ID for the new user ID
                merged_tweet_text = text
                current_user_id = user_id
                current_user_id_str = user_id_str
                current_tweet_id = tweet_id

    # Handle the last merged tweet if any
    if merged_tweet_text:
        merged_tweets[current_tweet_id]['merged'] = True
        merged_tweet_ids.add(current_tweet_id)

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['AirplaneMode']
airline_trees_collection = db['airline_trees']
merged_trees_collection = db['merged_trees']

# Fetch the documents from the collection
documents = airline_trees_collection.find({})

# Check if documents are found
if documents:
    # Initialize list to store merged trees
    merged_trees = []

    # Iterate over the documents
    for document in documents:
        # Extract tree ID and tree data from the document
        tree_id = document.get('tree_id')
        tree_data = document.get('tree_data', {})

        # Merge consecutive tweets with the same user ID and user ID string
        merged_tweets, merged_tweet_ids = merge_consecutive_tweets(tree_data)

        # Add the merged field to the existing tweet data
        for tweet_id, tweet_data in tree_data.items():
            if isinstance(tweet_data, dict):
                if tweet_id in merged_tweet_ids:
                    tweet_data['merged'] = True
                else:
                    tweet_data['merged'] = False

        # Add the merged tree to the list
        merged_trees.append({'tree_id': tree_id, 'tree_data': tree_data})

    # Store the merged trees in the new collection in MongoDB
    merged_trees_collection.insert_many(merged_trees)

else:
    print("No documents found in the collection.")








