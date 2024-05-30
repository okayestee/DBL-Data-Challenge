from bertopic import BERTopic
import pymongo
from Utility_functions import *
import gc

topic_model = BERTopic.load('bertopic_model')

topic_labels = {

-1: 'Undefined',
0: 'Delays and responses',
1: 'Empty',
2: 'Baggage',
3: 'Thanks',
4: 'Airplane jargon',
5: 'Seats and Booking',
6: 'Refund',
7: 'DM airline',
8: 'Qantas lounge',
9: 'Low cost airlines',
10: 'Food',
11: 'Bikes',
12: 'African Destinations',
13: 'Fat-shaming',
14: 'General',
15: 'Death',
16: 'Airfrance/police',
17: 'App/Browser',
18: 'The Queen',
19: 'Music',
20: 'Sustainability',
21: 'Travel Spots',
22: 'Lounge',
23: 'Twitter'
}

topic_model.set_topic_labels(topic_labels)

print(topic_model.get_topic_info())