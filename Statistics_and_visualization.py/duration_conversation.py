from datetime import datetime
from pymongo import MongoClient
import matplotlib.pyplot as plt
from collections import Counter

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['DBL']
valid_trees_collection = db['timeframe_trees_merged']

def get_conversation_duration(tree_data):
    # Extract the root tweet data
    root_tweet_data = tree_data.get('data')
    if not root_tweet_data:
        return 0
    
    # Get the creation timestamp of the root tweet
    root_created_at = datetime.strptime(root_tweet_data['created_at'], '%a %b %d %H:%M:%S %z %Y')

    # Initialize a variable to store the maximum child timestamp
    max_child_timestamp = root_created_at

    # Recursive function to traverse through the children tweets
    def traverse_children(node):
        nonlocal max_child_timestamp
        
        # Get the tweet data from the node
        tweet_data = node.get('data')
        if tweet_data:
            # Get the creation timestamp of the tweet
            tweet_created_at = datetime.strptime(tweet_data['created_at'], '%a %b %d %H:%M:%S %z %Y')
            if tweet_created_at > max_child_timestamp:
                max_child_timestamp = tweet_created_at

        # Recursively traverse through the children nodes
        for child_node in node.get('children', []):
            for user, child_data in child_node.items():
                traverse_children(child_data)

    # Start traversing through the children tweets
    traverse_children(tree_data)

    # Calculate the conversation duration in hours
    conversation_duration = (max_child_timestamp - root_created_at).total_seconds() / 3600.0

    return conversation_duration

def round_duration_to_nearest_hour(duration):
    return round(duration)

def calculate_all_conversation_durations(collection):
    conversation_durations_counter = Counter()
    # Iterate through all documents in the collection
    for document in collection.find():
        tree_data = document.get('tree_data')
        if tree_data:
            duration = get_conversation_duration(tree_data)
            # Include only conversations within 72 hours
            if 0 <= duration <= 50:
                rounded_duration = round_duration_to_nearest_hour(duration)
                conversation_durations_counter[rounded_duration] += 1
    return conversation_durations_counter

# Calculate conversation durations for all documents
conversation_durations_counter = calculate_all_conversation_durations(valid_trees_collection)

if conversation_durations_counter:
    print("Conversation Durations (hours):")
    for duration, count in sorted(conversation_durations_counter.items()):
        print(f"{duration} hours: {count} times")

    # Create a histogram plot
    plt.bar(list(conversation_durations_counter.keys()), list(conversation_durations_counter.values()), color='skyblue')
    plt.xlabel('Rounded Conversation Duration (hours)')
    plt.ylabel('Frequency')
    plt.title('Distribution of Conversation Durations (Rounded)')
    plt.grid(True)
    plt.show()
