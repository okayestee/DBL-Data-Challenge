from numpy import append, mean
import VADER_implementation as v_implement
import matplotlib.pyplot as plt
import numpy as np

# Get the VADER analyzer
analyzer = v_implement.analyzer
    
def get_reply_by_index(tree: dict, reply_index: int) -> dict:
    """
    Given a tree will get its a reply of a specified index.
    :param tree: the tree which contains the tweet and its replies
    :param reply_index: the index of the reply that should be returned
    :returns: the reply of the specified index
    """

    if 'children' in tree:
        reply_key = list(tree['children'][reply_index].keys())[0]
        reply = tree['children'][0][reply_key]
    else:
        reply = {}

    return reply

    
def get_convo(tree_doc: dict) -> list[dict]:
    """
    Generates and returns a conversation as a list of tweets.
    :param tree_doc: the document of the conversation tree
    :returns: conversation as a list of tweets
    """

    # Get the first tweet
    original_tweet = tree_doc['tree_data']
    original_tweet_data = original_tweet['data']

    # Initialize the conversation list
    convo: list = [original_tweet_data]

    # Initialize the next reply
    next_reply = get_reply_by_index(original_tweet, 0)

    # Recurse over the entire conversation and add each tweet visited to the conversation
    while len(next_reply) != 0:
        convo.append(next_reply['data'])
        next_reply = get_reply_by_index(next_reply, 0)
    
    # Make sure that conversation is valid by checking length
    if len(convo) >= 3:
        return convo
    else:
        return list()


# *OLD FUNCTION* ->
def extract_compounds_from_convo_VADER(conversation: list[dict]) -> list[float]: # REMOVE THIS WHEN CONVERSATIONS INCLUDE SENTIMENT VARIABLES
    convo_sentiments = list()

    for tweet in conversation:
        text = v_implement.get_full_text(tweet)
        sentiment_score = analyzer.polarity_scores(text)['compound']
        convo_sentiments.append(sentiment_score)

    return convo_sentiments

def extract_compounds_from_convo_vars(conversation: list[dict]) -> list[float]:
    """
    Returns the compound scores from all tweets in the conversation of a given tree.
    :param conversation: the conversation that contains the tweets 
    """
    convo_sentiments = list()

    for tweet in conversation:
        sentiment_score = tweet['compound sentiment']
        convo_sentiments.append(sentiment_score)

    return convo_sentiments

def get_evolutions(compounds: list[int]) -> list[list[str]]:
    """
    Based on a list of compound scores, creates and returns a list containing both all of the evolutions and non-evolutions.
    :param compounds: a list containing the compound scores of a conversation
    :returns: a list containing two lists, one being of the evolutions and the other being the non-evolutions (as strings in the format 'LABEL ---> LABEL')
    """

    # Initialize the lists
    sentiments = []
    evolutions: list[str] = list()
    non_evolutions: list[str] = list()

    # Label the sentiment
    for compound in compounds:
        if compound < -0.2:
            sentiments.append('NEGATIVE')
        elif compound > 0.25:
            sentiments.append('POSITIVE')
        else:
            sentiments.append('NEUTRAL')

    # Add all the sentiment patterns to either the evolutions or non-evolutions
    for index in range(0, len(sentiments)):
        if index + 1 != len(sentiments):
            if sentiments[index] != sentiments[index + 1]:
                evolutions.append(f'{sentiments[index]} ---> {sentiments[index + 1]}')
            else:
                non_evolutions.append(f'{sentiments[index]} ---> {sentiments[index + 1]}')

    return [evolutions, non_evolutions]

def count_evolution_types(list_of_compounds) -> dict[str, int]:
    """
    Counts the amount of sentiment evolutions and non-evolutions and returns the counts as a dictionary.
    :param list_of_compounds: a list containing the lists of compounds from each conversation
    :returns: a dictionary containing the counts for each evolution type with the type as keys and the counts as values
    """

    # Initialize all of the dictionary entries
    evolution_types: dict[str, int] = {}

    evolution_types['negative -> neutral'] = 0
    evolution_types['negative -> positive'] = 0
    evolution_types['neutral -> negative'] = 0
    evolution_types['neutral -> positive'] = 0
    evolution_types['positive -> neutral'] = 0
    evolution_types['positive -> negative'] = 0
    evolution_types['negative -> negative'] = 0
    evolution_types['neutral -> neutral'] = 0
    evolution_types['positive -> positive'] = 0


    for convo_compound_list in list_of_compounds:

        # If the list is not empty
        if len(convo_compound_list) > 0:

            # Get the evolution types
            all_evos = get_evolutions(convo_compound_list)
            evolutions = all_evos[0]
            non_evolutions = all_evos[1]
            
            # Count evolution types
            for evolution in evolutions:
                if evolution == 'NEGATIVE ---> NEUTRAL':
                    evolution_types['negative -> neutral'] += 1
                elif evolution == 'NEGATIVE ---> POSITIVE':
                    evolution_types['negative -> positive'] += 1
                elif evolution == 'NEUTRAL ---> NEGATIVE':
                    evolution_types['neutral -> negative'] += 1
                elif evolution == 'NEUTRAL ---> POSITIVE':
                    evolution_types['neutral -> positive'] += 1
                elif evolution == 'POSITIVE ---> NEGATIVE':
                    evolution_types['positive -> negative'] += 1
                elif evolution == 'POSITIVE ---> NEUTRAL':
                    evolution_types['positive -> neutral'] += 1

            for non_evo in non_evolutions:
                if non_evo == 'NEGATIVE ---> NEGATIVE':
                    evolution_types['negative -> negative'] += 1
                elif non_evo == 'NEUTRAL ---> NEUTRAL':
                    evolution_types['neutral -> neutral'] += 1
                elif non_evo == 'POSITIVE ---> POSITIVE':
                    evolution_types['positive -> positive'] += 1

    return evolution_types
    
def is_airline_userID(user_ID: int) -> bool:
    """
    Checks whether a user ID belongs to an airline.
    :param user_ID: the user ID to check
    :returns: a boolean value that represents the ID belonging to an airline
    """
    airline_userIDs = [56377143, 106062176, 18332190, 22536055, 124476322, 26223583, 2182373406, 38676903, 1542862735, 253340062, 218730857, 45621423, 20626359]
    if user_ID in airline_userIDs:
        return True
    else:
        return False

    
def get_tree_docs(collection, topic: str = '') -> list:
    """
    Given a collection of trees, gets a list of all of the documents containing a topic if specified. 
    If no topic is specified the list will contain all documents. 
    :param collection: the collection of trees
    :param topic: topic that the conversations in the trees must be about
    :returns: a list of tree documents (containing a topic if specified)
    """

    # Get all the documents from the collection
    all_tree_docs = list(collection.find({}))

    # Initialize the docs list
    tree_docs: list = []

    for tree_doc in all_tree_docs:

        # If a topic is specified, append only the docs about the topic
        if topic != '':
            if tree_doc['tree_data']['data']['topic'] == topic:
                tree_docs.append(tree_doc)        
        else:
            tree_docs.append(tree_doc)

    return tree_docs

def get_evolution_stats(tree_docs, desired_stats= 'combined') -> dict:
    """
    Calculates and returns the number of each evolution and non-evolution as a dictionary.
    :param collection: the MongoDB collection with trees that we should get the stats from
    :param desired_stats: what part of the data the statistics should be retrieved from. 'combined' by default, can be specified to either 'airline' or 'user'.
    :returns: a dictionary containing the counts of the evolutions from the specified part of the collection.
    """

    progress_counter = 0
    collection_size = len(tree_docs)

    all_compounds: list[list[float]] = list()
    airline_compounds: list[list[float]] = list()
    user_compounds: list[list[float]] = list()
    
    for tree in tree_docs:
        starting_user_id = tree['tree_data']['data']['user']['id']
        conversation = get_convo(tree)
        if is_airline_userID(starting_user_id):
            compound_scores = extract_compounds_from_convo_VADER(conversation) # CHANGE THIS TO THE extract_compound_from_convo_vars FORM ONCE SENTIMENT VARS HAVE BEEN ADDED
            airline_compound_scores = compound_scores[1::2]
            airline_compounds.append(airline_compound_scores)
            all_compounds.append(airline_compound_scores)

            progress_counter += 1

        else:
            compound_scores = extract_compounds_from_convo_VADER(conversation) # ALSO CHANGE THIS ONE LATER LIKE THE ONE ABOVE
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

def plot_evos(evolutions: dict[str, int], chart_type: str='pie', include_non_evos: bool = False):
    """
    Plots the given evolutions on a chart.
    :param evolutions: a dictionary containing the number of each evolution
    :param chart_type: the type of chart that should be plotted
    :param include_non_evos: a bool dictating whether to include non-evolutions (sentiments that do not change labels)
    """

    evo_types = list(evolutions.keys())
    evo_counts = list(evolutions.values())
    
    if include_non_evos == False:
        evo_types = evo_types[0:6]
        evo_counts = evo_counts[0:6]

    plt.figure(figsize=(12, 8))
    if chart_type == 'bar':
        plt.bar(evo_types, evo_counts)
        plt.ylabel('Number of Evolutions')
        plt.grid(axis='y', linestyle='--', alpha=0.7)  # Add horizontal grid lines
    else:
        plt.pie(evo_counts, labels=evo_types, autopct='%1.1f%%')

    plt.xlabel('Evolution Types')
    plt.title('Evolutions for Each Type')
    plt.xticks(rotation=45, ha='right')  # Rotate the x-axis labels for better readability
    plt.tight_layout()  # Adjust layout to prevent clipping of labels
    plt.show()

def get_increasing_decreasing_stats(evolutions: dict[str, int]) -> dict:

    inc_dec_stats = dict()
    
    inc_dec_stats['amount_increasing'] = evolutions['negative -> neutral'] + evolutions['negative -> positive'] + evolutions['neutral -> positive']
    inc_dec_stats['amount_decreasing'] = evolutions['neutral -> negative'] + evolutions['positive -> neutral'] + evolutions['positive -> negative']

    total_amount = 0
    for key in evolutions:
        total_amount += evolutions[key]
    
    total_evolutions = inc_dec_stats['amount_increasing'] + inc_dec_stats['amount_decreasing']
    
    inc_dec_stats['perc. increasing (all conversations)'] = round((inc_dec_stats['amount_increasing'] / total_amount) * 100, 2)
    inc_dec_stats['perc. decreasing (all conversations)'] = round((inc_dec_stats['amount_decreasing'] / total_amount) * 100, 2)
    inc_dec_stats['perc. increasing (only evolutions)'] = round((inc_dec_stats['amount_increasing'] / total_evolutions) * 100, 2)
    inc_dec_stats['perc. decreasing (only evolutions)'] = round((inc_dec_stats['amount_decreasing'] / total_evolutions) * 100, 2)

    return inc_dec_stats



def plot_evo_non_evo(evolutions: dict[str, int]) -> None:
    """
    Plots a pie chart of the evolutions vs the non-evolutions.
    :param evolutions: a dictionary containing the counts of each type of evolution and non-evolution
    """
    
    evo_values = list(evolutions.values())[0:6]
    non_evo_values = list(evolutions.values())[6:]

    total_evos = sum(evo_values)
    total_non_evos = sum(non_evo_values)
    total_both_types = [total_evos, total_non_evos]

    plt.figure(figsize=(12, 8))
    plt.pie(total_both_types, labels=['Evolutions', 'Non-evolutions'], autopct='%1.1f%%')

    plt.title('Evolutions VS Non-Evolutions')
    plt.xticks(rotation=45, ha='right')  # Rotate the x-axis labels for better readability
    plt.tight_layout()  # Adjust layout to prevent clipping of labels
    plt.show()



def plot_inc_dec(evolutions: dict[str, int], chart_type='pie'):
    """
    Plots a chart comparing the increasing and decreasing evolutions.
    :param evolutions: a dictionary containing the counts of each type of evolution and non-evolution
    """

    stats = get_increasing_decreasing_stats(evolutions)
    counts = list(stats.values())[0:2]

    plt.figure(figsize=(12, 8))
    if chart_type == 'bar':
        plt.bar(['Increasing', 'Decreasing'], counts)
        plt.ylabel('Number of Evolutions')
        plt.grid(axis='y', linestyle='--', alpha=0.7)  # Add horizontal grid lines
    else:
        plt.pie(counts, labels=['Increasing', 'Decreasing'], autopct='%1.1f%%')

    plt.title('Increasing VS Decreasing Sentiment Evolutions')
    plt.xticks(rotation=45, ha='right')  # Rotate the x-axis labels for better readability
    plt.tight_layout()  # Adjust layout to prevent clipping of labels
    plt.show()

def get_data_for_topics(collection, topic: str):
    """
    Gets the evo stats for a specific topic for a specific (airlines) collection.
    :param collection: the collection of conversations
    :param topic: the topic that the conversations must be about
    :returns: a dictionary containing the evo data
    """
    return get_evolution_stats(get_tree_docs(collection, topic))


def plot_increasing_per_topic_per_airline(topics: list[str], airlines: list[str], inc_percentages: list[list[float]]):

    # Number of categories and subcategories
    n_categories = len(topics)
    n_subcategories = len(airlines)

    # Width of bars
    bar_width = 0.2

    # X positions of categories
    category_indices = np.arange(n_categories)

    # Create a figure and axis
    fig, ax = plt.subplots()

    # Plot bars for each subcategory
    for i in range(n_subcategories):
        ax.bar(category_indices + i * bar_width, [inc_percentages[j][i] for j in range(n_categories)], 
            width=bar_width, label=airlines[i])

    # Set labels and title
    ax.set_xlabel('Topics')
    ax.set_ylabel('Percentage Increasing Sentiment Evolutions')
    ax.set_title('Increasing sentiment per Topic')
    ax.set_xticks(category_indices + bar_width * (n_subcategories - 1) / 2)
    ax.set_xticklabels(topics)
    ax.legend()
    ax.grid(axis='y', linestyle='--', alpha=0.7)  # Add horizontal grid lines


    # Show the plot
    plt.show()

plot_increasing_per_topic_per_airline(['Baggage', 'Delays', 'Cancellations'], ['American Air', 'KLM', 'Lufthansa'], [[10,20, 7], [40, 50, 35], [34,26,29]])
