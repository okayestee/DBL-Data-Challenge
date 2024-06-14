import Sentiment_evolution as senti_evo
import pymongo


# Connect to the database
client = pymongo.MongoClient("mongodb://localhost:27017/") ## Connect to MongoDB
db = client['DBL_data'] ## Use the DBL database
collection = db['Airline_convos'] ## Choose a collection of conversations


# Get the evolution statistics
evolution_statistics = senti_evo.get_evolution_stats(senti_evo.get_tree_docs(collection))

# Or get the evolution statistics for a specific topic
#evolution_stats_for_topic = senti_evo.get_evolution_stats(senti_evo.get_tree_docs(collection, 'Baggage'))

# Print the exact counts in the terminal
print(evolution_statistics)

# Print the distribution of increasing and decreasing evolutions
print(senti_evo.get_increasing_decreasing_stats(evolution_statistics))

# Create a bar chart of the evolutions
senti_evo.plot_evos(evolution_statistics)
