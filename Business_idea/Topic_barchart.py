from bertopic import BERTopic
import pymongo
from Utility_functions import *

topic_model = BERTopic.load('merged_model')
fig = topic_model.visualize_barchart( top_n_topics=46, n_words=5, custom_labels=True, title='Topic Word Scores', width=250, height=250, autoscale=False)
fig.write_html("barchart.html")