from click import progressbar
from numpy import append, mean
import VADER_implementation as v_implement
import matplotlib.pyplot as plt


analyzer = v_implement.analyzer

def get_full_text(tweet) -> str:
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

    
def get_convo(tree_doc: dict) -> list:

    original_tweet = tree_doc['tree_data']
    original_tweet_text = original_tweet['data']

    convo: list = [original_tweet_text]
    next_reply = get_reply_by_index(original_tweet, 0)

    while len(next_reply) != 0:
        convo.append(next_reply['data'])
        next_reply = get_reply_by_index(next_reply, 0)
    
    if len(convo) >= 3:
        return convo
    else:
        return list()

def extract_compound_from_convo_VADER(tree) -> list[int]:
    conversation = get_convo(tree)
    convo_sentiments = list()

    for tweet in conversation:
        text = get_full_text(tweet)
        sentiment_score = analyzer.polarity_scores(text)['compound']
        convo_sentiments.append(sentiment_score)

    return convo_sentiments

def extract_compound_from_convo_vars(tree) -> list[int]:
    conversation = get_convo(tree)
    convo_sentiments = list()

    for tweet in conversation:
        sentiment_score = tweet['compound sentiment']
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

def count_evolution_types(list_of_compounds) -> dict[str, int]:
    evolution_types: dict[str, int] = {}

    evolution_types['neg_to_neu'] = 0
    evolution_types['neg_to_pos'] = 0
    evolution_types['neu_to_neg'] = 0
    evolution_types['neu_to_pos'] = 0
    evolution_types['pos_to_neu'] = 0
    evolution_types['pos_to_neg'] = 0
    evolution_types['neg_to_neg'] = 0
    evolution_types['neu_to_neu'] = 0
    evolution_types['pos_to_pos'] = 0

    for convo_compound_list in list_of_compounds:
        if len(convo_compound_list) > 0:
            all_evos = get_evolutions(convo_compound_list)
            evolutions = all_evos[0]
            
            # Count evolution types
            for evolution in evolutions:
                if evolution == 'NEGATIVE ---> NEUTRAL':
                    evolution_types['neg_to_neu'] += 1
                elif evolution == 'NEGATIVE ---> POSITIVE':
                    evolution_types['neg_to_pos'] += 1
                elif evolution == 'NEUTRAL ---> NEGATIVE':
                    evolution_types['neu_to_neg'] += 1
                elif evolution == 'NEUTRAL ---> POSITIVE':
                    evolution_types['neu_to_pos'] += 1
                elif evolution == 'POSITIVE ---> NEGATIVE':
                    evolution_types['pos_to_neg'] += 1
                elif evolution == 'POSITIVE ---> NEUTRAL':
                    evolution_types['pos_to_neu'] += 1

            for non_evo in all_evos[1]:
                if non_evo == 'NEGATIVE ---> NEGATIVE':
                    evolution_types['neg_to_neg'] += 1
                elif non_evo == 'NEUTRAL ---> NEUTRAL':
                    evolution_types['neu_to_neu'] += 1
                elif non_evo == 'POSITIVE ---> POSITIVE':
                    evolution_types['pos_to_pos'] += 1
            
    # print(f'Non-evolutions: \nNEGATIVE ---> NEGATIVE - {neg_to_neg}\nNEUTRAL ---> NEUTRAL - {neu_to_neu}\nPOSITIVE ---> POSITIVE - {pos_to_pos}\n')

    return evolution_types
    
    # print(f'Evolutions: \nNEGATIVE ---> NEUTRAL - {neg_to_neu}\nNEGATIVE ---> POSITIVE - {neg_to_pos}\nNEUTRAL ---> NEGATIVE - {neu_to_neg}\nNEUTRAL ---> POSITIVE - {neu_to_pos}\nPOSITIVE ---> NEGATIVE - {pos_to_neg}\nPOSITIVE ---> NEUTRAL - {pos_to_neu}\n')

def is_airline_userID(user_ID: int) -> bool:
    airline_userIDs = [56377143, 106062176, 18332190, 22536055, 124476322, 26223583, 2182373406, 38676903, 1542862735, 253340062, 218730857, 45621423, 20626359]
    if user_ID in airline_userIDs:
        return True
    else:
        return False



def get_evolution_stats(collection, desired_stats= 'combined') -> dict:
    """
    Calculates and returns the number of each evolution and non-evolution.
    :param collection: the MongoDB collection with trees that we should get the stats from
    :param desired_stats: what part of the data the statistics should be retrieved from. 'combined' by default, can be specified to either 'airline' or 'user'.
    :returns: a dictionary containing the counts of the evolutions from the specified part of the collection.
    """


    trees = list(collection.find({}))
    progress_counter = 0
    collection_size = len(trees)

    all_compounds: list[list[int]] = list()
    airline_compounds: list[list[int]] = list()
    user_compounds: list[list[int]] = list()
    
    for tree in trees:
        starting_user_id = tree['tree_data']['data']['user']['id']
        if is_airline_userID(starting_user_id):
            compound_scores = extract_compound_from_convo_VADER(tree) # CHANGE THIS TO THE extract_compound_from_convo_vars FORM ONCE SENTIMENT VARS HAVE BEEN ADDED
            airline_compound_scores = compound_scores[1::2]
            airline_compounds.append(airline_compound_scores)
            all_compounds.append(airline_compound_scores)

            progress_counter += 1

        else:
            compound_scores = extract_compound_from_convo_VADER(tree) # ALSO CHANGE THIS ONE LATER LIKE THE ONE ABOVE
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
        return count_evolution_types(airline_compounds)
    elif desired_stats == 'user':
        print('User conversations:')
        return count_evolution_types(user_compounds)
    else:
        print('Combined conversations:')
        return count_evolution_types(all_compounds)

def plot_evos(evolutions: dict[str, int], include_non_evos: bool = False):
    """
    Plots the given evolutions on a bar chart.
    :param evolutions: a dictionary containing the number of each evolution
    :param include_non_evos: a bool dictating whether to include non-evolutions (sentiments that do not change labels)
    """


    evo_types = list(evolutions.keys())
    evo_counts = list(evolutions.values())
    
    if include_non_evos == False:
        evo_types = evo_types[0:6]
        evo_counts = evo_counts[0:6]

    # Create a vertical bar chart
    plt.figure(figsize=(12, 8))
    plt.bar(evo_types, evo_counts, color='skyblue')
    plt.xlabel('Evolution Types')
    plt.ylabel('Number of Evolutions')
    plt.title('Number of Evolutions for Each Type')
    plt.xticks(rotation=45, ha='right')  # Rotate the x-axis labels for better readability
    plt.grid(axis='y', linestyle='--', alpha=0.7)  # Add horizontal grid lines
    plt.tight_layout()  # Adjust layout to prevent clipping of labels
    plt.show()