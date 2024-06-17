from math import ceil
from statistics import mean
import matplotlib.pyplot as plt



def get_sentiment_stats(collection) -> dict:
    """
    Generates and returns a dictionary containing statistics about the sentiment in a given collection.
    :param collection: the collection of tweets to analyze
    :returns: a dictionary containing the statistics
    """

    # Initialize the lists for means
    batch_mean_compound: list[float] = []
    batch_mean_compound_pos: list[float] = []
    batch_mean_compound_neu: list[float] = []
    batch_mean_compound_neg: list[float] = []

    # Initialize the statistics dictionary
    sentiment_stats: dict[str, float] = dict()

    sentiment_stats['positive sentiments'] = 0
    sentiment_stats['negative sentiments'] = 0
    sentiment_stats['neutral sentiments'] = 0

    batch_size = 10000
    documents_processed = 0

    while True:
        # Retrieve a batch of documents
        batch = list(collection.find({}).skip(documents_processed).limit(batch_size))
        if not batch:
            print('Sentiment Analysis Finished!')
            break  # Exit loop if no more documents are retrieved

        # Add a new entry into each of the means lists
        batch_mean_compound.append(0)
        batch_mean_compound_pos.append(0)
        batch_mean_compound_neu.append(0)
        batch_mean_compound_neg.append(0)

        current_batch_number = documents_processed // batch_size

        # Initialize the counters for the current batch
        neg_counter = 0
        neu_counter = 0
        pos_counter = 0

        
        for document in batch:
            # Get the compound score of the tweet
            compound_score: float = float(document.get('compound sentiment'))

            # Get the sentiment label of the tweet
            if compound_score < -0.2:
                sentiment_stats['negative sentiments'] += 1
                neg_counter += 1
                batch_mean_compound_neg[current_batch_number] += compound_score
            elif compound_score > 0.25:
                sentiment_stats['positive sentiments'] += 1
                pos_counter += 1
                batch_mean_compound_pos[current_batch_number] += compound_score
            else:
                sentiment_stats['neutral sentiments'] += 1
                neu_counter += 1
                batch_mean_compound_neu[current_batch_number] += compound_score

            batch_mean_compound[current_batch_number] += compound_score
            

        # Set the elements of the current batch to the mean score
        batch_mean_compound[current_batch_number] /= batch_size

        if neg_counter > 0:
            batch_mean_compound_neg[current_batch_number] /= neg_counter
        if neu_counter > 0:
            batch_mean_compound_neu[current_batch_number] /= neu_counter
        if pos_counter > 0:
            batch_mean_compound_pos[current_batch_number] /= pos_counter

        documents_processed += len(batch)
        print(documents_processed)

    # Add the mean of the means to the dictionary
    sentiment_stats['mean compound'] = mean(batch_mean_compound)
    sentiment_stats['mean negative compound'] = mean(batch_mean_compound_neg)
    sentiment_stats['mean neutral compound'] = mean(batch_mean_compound_neu)
    sentiment_stats['mean positive compound'] = mean(batch_mean_compound_pos)

    return sentiment_stats



def plot_sentiment_stats(sentiment_stats: dict, chart_type='pie') -> None:

    """
    Plots the given tweets their sentiment distribtuion on a bar chart.
    :param sentiment_stats: a dictionary containing the number of each sentiment type
    :param show_means: a bool dictating whether to show the sentiment counts or means
    """
    
    # Get the sentiment data from the statistics dictionary
    sentiment_types = list(sentiment_stats.keys())[0:3]
    sentiment_counts = list(sentiment_stats.values())[0:3]

    # Create a vertical bar chart
    plt.figure(figsize=(12, 8))
    if chart_type == 'bar':
        plt.bar(sentiment_types, sentiment_counts, color='skyblue')
        plt.ylabel('Number of Tweets with Type')
    else:
        plt.pie(sentiment_counts, labels=sentiment_types, autopct='%1.1f%%')

    plt.xlabel('Sentiment Types')
    plt.title('Tweets for Each Sentiment Type')
    plt.xticks(rotation=45, ha='right')  # Rotate the x-axis labels for better readability
    plt.grid(axis='y', linestyle='--', alpha=0.7)  # Add horizontal grid lines
    plt.tight_layout()  # Adjust layout to prevent clipping of labels
    plt.show()



def plot_means(sentiment_stats: dict) -> None:
    """
    Creates a chart for the mean compound scores
    :param sentiment_stats: the sentiment statistics used to create the plot
    """

    # Get the means from the statistics dictionary
    mean_types = list(sentiment_stats.keys())[0:3]
    mean_counts = list(sentiment_stats.values())[0:3]

    plt.figure(figsize=(12, 8))
    plt.bar(mean_types, mean_counts, color='skyblue')
    plt.ylabel('Mean Compound Score')
    plt.xlabel('Sentiment Labels')
    plt.title('Mean Ccore per Label')
    plt.xticks(rotation=45, ha='right')  # Rotate the x-axis labels for better readability
    plt.grid(axis='y', linestyle='--', alpha=0.7)  # Add horizontal grid lines
    plt.tight_layout()  # Adjust layout to prevent clipping of labels
    plt.show()