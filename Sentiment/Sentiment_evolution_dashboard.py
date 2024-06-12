import Sentiment_evolution as senti_evo
import pymongo


# Connect to the database
client = pymongo.MongoClient("mongodb://localhost:27017/") # Connect to MongoDB
db = client['DBL_data'] # Use the DBL database
collection = db['Airline_convos'] # Choose a collection of conversations

# Print the evolution statistics of the chosen collection
senti_evo.get_evolution_stats(collection, 'airline')
