import Sentiment_evolution as senti_evo
import pymongo

# Connect to the database
client = pymongo.MongoClient("mongodb://localhost:27017/") ## Connect to MongoDB
db = client['DBL'] ## Use the DBL database
<<<<<<< HEAD
collection = db['Airline_convos'] ## Choose a collection of conversations
=======
collection = db['valid_trees_airline_X'] ## Choose a collection of conversations
>>>>>>> 0657ac9c82b7320df106f6683b9e9043df2b44ab

# Get the evolution statistics
evolution_statistics = senti_evo.get_evolution_stats(senti_evo.get_tree_docs(collection))

topic_evolution_stats = senti_evo.get_evolution_stats(senti_evo.get_tree_docs(collection, 'Baggage')) ## Fill in the name of the desired topic

# Print the exact counts in the terminal
print(evolution_statistics)
print(topic_evolution_stats)

# Print the distribution of increasing and decreasing evolutions
print(senti_evo.get_increasing_decreasing_stats(evolution_statistics))

# Create a bar chart of the evolutions
senti_evo.plot_evos(evolution_statistics)
