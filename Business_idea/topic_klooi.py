from bertopic import BERTopic
from Utility_functions import *

topic_model = BERTopic.load('merged_model')
tweet = get_random_tweet(1)[0]

print(new_labels(tweet['topic'],topic_model.custom_labels_),'|',tweet['text'])