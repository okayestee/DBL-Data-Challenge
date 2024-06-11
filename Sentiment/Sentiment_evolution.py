from click import progressbar
from numpy import append, mean
import pymongo
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import Random_sample as rs


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['DBL_data']


def get_full_text(tweet):
    if tweet.get('truncated', True):
        return tweet.get('extended_tweet', {}).get('full_text', '')
    else:
        return tweet.get('text', '')
    
def get_replies(tree_data: dict) -> list:

    replies: list = []

    for reply_index in range(0, len(tree_data['children'])):
        replies.append(get_reply_by_index(tree_data, reply_index))


    return replies


def get_reply_by_index(tree: dict, reply_index: int):

    if 'children' in tree:

        reply_key = list(tree['children'][reply_index].keys())[0]
        reply = tree['children'][0][reply_key]  
    else:
        reply = {}

    return reply

    
def get_convo(tree_doc: dict) -> list[str]:

    original_tweet = tree_doc['tree_data']
    original_tweet_text = get_full_text(original_tweet['data'])

    convo: list = [original_tweet_text]
    next_reply = get_reply_by_index(original_tweet, 0)

    while len(next_reply) != 0:
        convo.append(get_full_text(next_reply['data']))
        next_reply = get_reply_by_index(next_reply, 0)
    
    if len(convo) >= 3:
        return convo
    else:
        return list()

def extract_compound_from_convo(tree) -> list[int]:
    conversation = get_convo(tree)
    convo_sentiments = list()

    for tweet in conversation:
        text = str(tweet)
        sentiment_score = analyzer.polarity_scores(text)['compound']
        convo_sentiments.append(sentiment_score)

    return convo_sentiments

def get_evolutions(user_compounds: list[int]) -> list[list[str]]:
    
    sentiments = []
    evolutions: list[str] = list()
    non_evolutions: list[str] = list()

    # Label the sentiment=
    for compound in user_compounds:
        if compound < -0.2:
            sentiments.append('NEGATIVE')
        elif compound > 0.25:
            sentiments.append('POSITIVE')
        else:
            sentiments.append('NEUTRAL')

    for index in range(0, len(sentiments)):
        if index + 1 != len(sentiments):
            if sentiments[index] != sentiments[index + 1]:
                evolutions.append(f'{sentiments[index]} ---> {sentiments[index + 1]}')
            else:
                non_evolutions.append(f'{sentiments[index]} ---> {sentiments[index + 1]}')

    return [evolutions, non_evolutions]

def count_evolution_types(list_of_compounds) -> str:
    neg_to_neu = 0
    neg_to_pos = 0
    neu_to_neg = 0
    neu_to_pos = 0
    pos_to_neu = 0
    pos_to_neg = 0

    neg_to_neg = 0
    neu_to_neu = 0
    pos_to_pos = 0

    for convo_compound_list in list_of_compounds:
        if len(convo_compound_list) > 0:
            all_evos = get_evolutions(convo_compound_list)
            evolutions = all_evos[0]
            
            # Count evolution types
            for evolution in evolutions:
                if evolution == 'NEGATIVE ---> NEUTRAL':
                    neg_to_neu += 1
                elif evolution == 'NEGATIVE ---> POSITIVE':
                    neg_to_pos += 1
                elif evolution == 'NEUTRAL ---> NEGATIVE':
                    neu_to_neg += 1
                elif evolution == 'NEUTRAL ---> POSITIVE':
                    neu_to_pos += 1
                elif evolution == 'POSITIVE ---> NEGATIVE':
                    pos_to_neg += 1
                elif evolution == 'POSITIVE ---> NEUTRAL':
                    pos_to_neu += 1

            for non_evo in all_evos[1]:
                if non_evo == 'NEGATIVE ---> NEGATIVE':
                    neg_to_neg += 1
                elif non_evo == 'NEUTRAL ---> NEUTRAL':
                    neu_to_neu += 1
                elif non_evo == 'POSITIVE ---> POSITIVE':
                    pos_to_pos += 1
            
    print(f'NEGATIVE ---> NEGATIVE - {neg_to_neg}\nNEUTRAL ---> NEUTRAL - {neu_to_neu}\n POSITIVE ---> POSITIVE - {pos_to_pos}')

    
    return f'NEGATIVE ---> NEUTRAL - {neg_to_neu}\nNEGATIVE ---> POSITIVE - {neg_to_pos}\nNEUTRAL ---> NEGATIVE - {neu_to_neg}\nNEUTRAL ---> POSITIVE - {neu_to_pos}\nPOSITIVE ---> NEGATIVE - {pos_to_neg}\nPOSITIVE ---> NEUTRAL - {pos_to_neu}'



analyzer = SentimentIntensityAnalyzer()
analyzer = rs.update_VADER(analyzer)

all_compounds: list[list[int]] = list()
airline_compounds: list[list[int]] = list()
user_compounds: list[list[int]] = list()


# Get the airline conversation scores
collection = db['Airline_convos']

airline_trees = list(collection.find({}))

progress_counter = 0

for tree in airline_trees:
    compound_scores = extract_compound_from_convo(tree)
    airline_compound_scores = compound_scores[1::2]
    print(f'Full: {compound_scores} \n User: {airline_compound_scores}')
    airline_compounds.append(airline_compound_scores)
    all_compounds.append(airline_compound_scores)

    progress_counter += 1
    print(f'{progress_counter} / 111')


print('AIRLINES FINISHED')


# Get the user conversation scores
collection = db['User_convos']

user_trees = list(collection.find({}))

progress_counter = 0


for tree in user_trees:
    compound_scores = extract_compound_from_convo(tree)
    user_compound_scores = compound_scores[0::2]
    user_compounds.append(user_compound_scores)
    all_compounds.append(user_compound_scores)

    progress_counter += 1
    print(f'{progress_counter} / 26493')

print('USERS FINISHED')

print(f'Airlines: {count_evolution_types(airline_compounds)}')
print(f'Users: {count_evolution_types(user_compounds)}')
print(f'All convos: {count_evolution_types(all_compounds)}')




    