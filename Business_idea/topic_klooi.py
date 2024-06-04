from bertopic import BERTopic
from Utility_functions import *

topic_model = BERTopic.load('merged_model')
tweet_list = get_random_tweet(1)
for tweet in tweet_list:
    text, topic = get_full_text(tweet), get_real_label(tweet['topic'], topic_model.custom_labels_)
    print(topic,'|', text, '\n')