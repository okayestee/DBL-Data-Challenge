from click import progressbar
from numpy import append, mean
import VADER_implementation as v_implement


analyzer = v_implement.analyzer

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

def count_evolution_types(list_of_compounds):
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
            
    print(f'Non-evolutions: \nNEGATIVE ---> NEGATIVE - {neg_to_neg}\nNEUTRAL ---> NEUTRAL - {neu_to_neu}\nPOSITIVE ---> POSITIVE - {pos_to_pos}\n')

    
    print(f'Evolutions: \nNEGATIVE ---> NEUTRAL - {neg_to_neu}\nNEGATIVE ---> POSITIVE - {neg_to_pos}\nNEUTRAL ---> NEGATIVE - {neu_to_neg}\nNEUTRAL ---> POSITIVE - {neu_to_pos}\nPOSITIVE ---> NEGATIVE - {pos_to_neg}\nPOSITIVE ---> NEUTRAL - {pos_to_neu}\n')

def is_airline_userID(user_ID: int) -> bool:
    airline_userIDs = [56377143, 106062176, 18332190, 22536055, 124476322, 26223583, 2182373406, 38676903, 1542862735, 253340062, 218730857, 45621423, 20626359]
    if user_ID in airline_userIDs:
        return True
    else:
        return False



def get_evolution_stats(collection, desired_stats= 'all') -> None:

    trees = list(collection.find({}))
    progress_counter = 0
    collection_size = len(trees)

    all_compounds: list[list[int]] = list()
    airline_compounds: list[list[int]] = list()
    user_compounds: list[list[int]] = list()
    
    for tree in trees:
        starting_user_id = tree['tree_data']['data']['user']['id']
        if is_airline_userID(starting_user_id):
            compound_scores = extract_compound_from_convo(tree)
            airline_compound_scores = compound_scores[1::2]
            airline_compounds.append(airline_compound_scores)
            all_compounds.append(airline_compound_scores)

            progress_counter += 1

        else:
            compound_scores = extract_compound_from_convo(tree)
            user_compound_scores = compound_scores[0::2]
            user_compounds.append(user_compound_scores)
            all_compounds.append(user_compound_scores)

            progress_counter += 1

        # Give an update every 10% of progress
        if progress_counter % (collection_size // 10) == 0:
            print(f'Progress: {progress_counter} / {collection_size}')

    # Print the results to the terminal
    if desired_stats == 'airline':
        print('Airline conversations:')
        count_evolution_types(airline_compounds)
    elif desired_stats == 'user':
        print('User conversations:')
        count_evolution_types(user_compounds)
    elif desired_stats == 'combined':
        print('Combined conversations:')
        count_evolution_types(all_compounds)
    else:
        print('Airline conversations:')
        count_evolution_types(airline_compounds)

        print('User conversations:')
        count_evolution_types(user_compounds)

        print('Combined conversations:')
        count_evolution_types(all_compounds)

    