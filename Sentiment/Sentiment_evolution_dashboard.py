import Sentiment_evolution as senti_evo
import pymongo

# Connect to the database
client = pymongo.MongoClient("mongodb://localhost:27017/") ## Connect to MongoDB
db = client['DBL'] ## Use the DBL database
collection = db['valid_trees_airline_X'] ## Choose a collection of conversations

# Get the evolution statistics
evolution_statistics = senti_evo.get_evolution_stats(senti_evo.get_tree_docs(collection))

# Print the exact counts in the terminal
print(evolution_statistics)
#print(topic_evolution_stats)

# Print the distribution of increasing and decreasing evolutions
print(senti_evo.get_increasing_decreasing_stats(evolution_statistics))

# Create a bar chart of the evolutions
senti_evo.plot_evos(evolution_statistics)


# Get the topic sentiment evolution chart
topics = ['Baggage', 'Delay'] ## Fill in the topics we're looking for
airline_collection_names = ['American', 'AirFrance', 'Lufthanse'] ## Make sure the collection names match for each airline



# No need to edit the stuff below
# _____________________________________________________________________
dictionary_topics: dict[str, dict[str, float]] = {}

for topic in topics:
    dictionary_topics[topic] = dict()
    for airline in airline_collection_names:
        dictionary_topics[topic][airline] = senti_evo.get_data_for_topics(db[airline], topic)['perc. increasing (only evolutions)']

inc_percentages: list[list[float]] = []

for topic in topics:
    perc_list = list()
    for airline in airline_collection_names:
        perc_list.append(dictionary_topics[topic][airline])
    inc_percentages.append(perc_list)

senti_evo.plot_increasing_per_topic_per_airline(topics, airline_collection_names, inc_percentages)

