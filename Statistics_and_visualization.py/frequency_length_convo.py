from datetime import datetime
from pymongo import MongoClient
import matplotlib.pyplot as plt
from collections import Counter

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['DBL']
valid_trees_collection = db['timeframe_trees_merged']

def get_conversation_length(tree_data):
    # Recursive function to calculate the length of the conversation
    def calculate_length(node, current_length):
        nonlocal max_length
        
        # Increment the length for each child node
        current_length += 1
        
        # Update the max length if the current length is greater
        if current_length > max_length:
            max_length = current_length
        
        # Recursively calculate the length for each child node
        for child_node in node.get('children', []):
            for user, child_data in child_node.items():
                calculate_length(child_data, current_length)
    
    # Initialize max_length
    max_length = 0
    
    # Start calculating the length of the conversation
    calculate_length(tree_data, 0)
    
    return max_length

def calculate_all_conversation_lengths(collection):
    conversation_lengths_counter = Counter()
    # Iterate through all documents in the collection
    for document in collection.find():
        tree_data = document.get('tree_data')
        if tree_data:
            length = get_conversation_length(tree_data)
            conversation_lengths_counter[length] += 1
    return conversation_lengths_counter

# Calculate conversation lengths for all documents
conversation_lengths_counter = calculate_all_conversation_lengths(valid_trees_collection)

if conversation_lengths_counter:
    print("Conversation Lengths:")
    for length, count in sorted(conversation_lengths_counter.items()):
        print(f"Length {length}: {count} times")

    # Create a histogram plot
    plt.bar(list(conversation_lengths_counter.keys()), list(conversation_lengths_counter.values()), color='orange')
    plt.xlabel('Conversation Length (number of nodes)')
    plt.ylabel('Frequency')
    plt.title('Distribution of Conversation Lengths')
    plt.grid(True)
    plt.show()
else:
    print("No conversation data found.")




