import matplotlib.pyplot as plt
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

def compare_collections_and_visualize(db: Database, collection_name_1: str, collection_name_2: str) -> None:
    """
    Compares the number of documents in two collections and creates a bar chart visualization.

    Parameters:
    db (Database): The MongoDB database instance.
    collection_name_1 (str): The name of the first collection to compare.
    collection_name_2 (str): The name of the second collection to compare.
    
    Returns:
    None
    """
    # Connect to the collections
    collection_1: Collection = db[collection_name_1]
    collection_2: Collection = db[collection_name_2]

    # Get the number of documents in each collection
    count_1 = collection_1.count_documents({})
    count_2 = collection_2.count_documents({})

    # Prepare data for the bar chart
    labels = ['User trees', 'Airline trees']
    counts = [count_1, count_2]

    # Create the bar chart with customized colors and smaller width
    plt.figure(figsize=(10, 6))
    bars = plt.bar(labels, counts, color=['orange', 'skyblue'], width=0.4)  # Adjust width here (default is 0.8)
    plt.xlabel('Trees')
    plt.ylabel('Number of Trees')
    plt.title('Comparison of Tree Counts in Collections')

    # Add horizontal grid lines
    plt.grid(axis='y')

    # Adjust spacing between bars
    plt.subplots_adjust(bottom=0.2)

    # Show count values on top of bars
    for bar, count in zip(bars, counts):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3, str(count), ha='center', va='bottom')  # Convert count to str

    plt.show()

# Example usage:
client = MongoClient('mongodb://localhost:27017/')
db = client['DBL']

# Assuming 'timeframe_trees_user' and 'timeframe_trees_airline' are the collections to compare
compare_collections_and_visualize(db, 'timeframe_trees_user', 'timeframe_trees_airline')
