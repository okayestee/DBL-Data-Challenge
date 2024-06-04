from bertopic import BERTopic
import pymongo
from Utility_functions import *
import gc
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.pipeline import make_pipeline
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer

pipe = make_pipeline(
    TfidfVectorizer(),
    TruncatedSVD(100)
)


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client.Airline_data
collection = db.removed_duplicates






#Batch_size to reduce memory usage
batch_size = 80000


# Initialize an empty list to collect all preprocessed tweets
preprocessed_tweets = [clean(get_full_text(tweet)) for tweet in get_random_tweet(batch_size)] 
gc.collect()
client.close()
# Process and collect all tweets in batches

vectorizer_model = CountVectorizer(stop_words="english")
topic_model = BERTopic(language='english', min_topic_size= 200, n_gram_range= (1, 2), top_n_words= 10, zeroshot_min_similarity= 0.7,vectorizer_model=vectorizer_model)
# Fit the model on the combined preprocessed tweets
topics, probs = topic_model.fit_transform(preprocessed_tweets)


print(topic_model.get_topic_info())

#Save model to a file
topic_model.save("n_gram_bertopic_model")
