from bertopic import BERTopic
import pymongo
from Utility_functions import *
import gc

topic_model = BERTopic.load('merged_model')

topic_labels = ['Undefined','Response', 'Qantas', 'Posted', 'Ryanair', 'Baggage', 'Easyjet', 'Avgeek', 'Lufthansa', 'Seats', 'Delays', 'Racial Retweet', 'Coronavirus','Cancelled','Booking reference', 'Airline','Refunds','KLM','Thank you for Flight','Roundtrip','Retweet backless seats', 'Twitter jargon','Retweet pride parade','Food','Retweet Breastfeeding','Retweet Lesbian','Climate','Retweet Lufthansa Iran','General Retweet','Corporate affairs','Racism','Locating baggage','Racial Retweet','Refunds','Retweet muslims','Italy','Retweet non-airline incident','Places in england','Retweet BTS','Flying','Retweet discrimination corona','Retweet lesbian sexualization','Flight tracker','Workers and Catering','Claims','Retweet Nigerian lawyer','Retweet Terrorism','Europa AirFrance']
topic_model.set_topic_labels(topic_labels)
print(topic_model.get_topic_info())
topic_model.save('merged_model')