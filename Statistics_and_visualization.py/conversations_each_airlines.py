from pymongo import MongoClient
from collections import defaultdict
import matplotlib.pyplot as plt

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['AirplaneMode']
valid_trees_collection = db['valid_trees_merged']

# Airline user IDs
airline_ids = {
    'KLM': '56377143',
    'AirFrance': '106062176',
    'British_Airways': '18332190',
    'AmericanAir': '22536055',
    'Lufthansa': '124476322',
    'AirBerlin': '26223583',
    'AirBerlin_assist': '2182373406',
    'easyJet': '38676903',
    'RyanAir': '1542862735',
    'SingaporeAir': '253340062',
    'Qantas': '218730857',
    'EtihadAirways': '45621423',
    'VirginAtlantic': '20626359'
}

def contains_specific_airline_user(tree_data, airline_id):
    # Recursive function to traverse through the children tweets
    def traverse_children(node):
        # Check if the current node has a user ID that matches the specific airline ID
        tweet_data = node.get('data')
        if tweet_data:
            user_data = tweet_data.get('user')
            if user_data and user_data.get('id_str') == airline_id:
                return True
        
        # Recursively traverse through the children nodes
        for child_node in node.get('children', []):
            for user, child_data in child_node.items():
                if traverse_children(child_data):
                    return True
        
        return False

    # Start traversing through the children tweets
    return traverse_children(tree_data)

def count_conversations_for_airlines(collection, airline_ids):
    airline_conversations_count = defaultdict(int)
    # Iterate through all documents in the collection
    for document in collection.find():
        tree_data = document.get('tree_data')
        if tree_data:
            for airline, user_id in airline_ids.items():
                if contains_specific_airline_user(tree_data, user_id):
                    airline_conversations_count[airline] += 1
                    break  # Once a match is found, move to the next document
    return airline_conversations_count

# Count the number of conversations for each airline
airline_conversations_count = count_conversations_for_airlines(valid_trees_collection, airline_ids)

print("Number of conversations for each airline:")
for airline, count in airline_conversations_count.items():
    print(f"{airline}: {count}")

# Data for visualization
airlines = list(airline_conversations_count.keys())
counts = list(airline_conversations_count.values())

# Create a vertical bar chart
plt.figure(figsize=(12, 8))
plt.bar(airlines, counts, color='skyblue')
plt.xlabel('Airlines')
plt.ylabel('Number of Conversations')
plt.title('Number of Conversations for Each Airline')
plt.xticks(rotation=45, ha='right')  # Rotate the x-axis labels for better readability
plt.grid(axis='y', linestyle='--', alpha=0.7)  # Add horizontal grid lines
plt.tight_layout()  # Adjust layout to prevent clipping of labels
plt.show()
