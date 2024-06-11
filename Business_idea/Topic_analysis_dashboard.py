from Topic_analysis import topic_analysis
from bertopic import BERTopic

# To do the topic analysis please run this code

# fill in your database name
db = 'Airline Data'

#fill in you collection name you want to do the analysis on
collection = 'no-inconsistency'

# the model you want to use to do the analysis
model = BERTopic.load('merged_model')

topic_analysis(db, collection, model)
