from math import ceil
from statistics import mean
import matplotlib.pyplot as plt


def get_sentiment_stats(collection) -> dict:

    batch_mean_compound: list[float] = []
    batch_mean_compound_pos: list[float] = []
    batch_mean_compound_neu: list[float] = []
    batch_mean_compound_neg: list[float] = []

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

        batch_mean_compound.append(0)
        batch_mean_compound_pos.append(0)
        batch_mean_compound_neu.append(0)
        batch_mean_compound_neg.append(0)
        
        for document in batch:

            compound_score = document.get('compound sentiment')

            neg_counter = 0
            neu_counter = 0
            pos_counter = 0

            if compound_score < -0.2:
                sentiment_stats['negative sentiments'] += 1
                neg_counter += 1
                batch_mean_compound_neg[(documents_processed // batch_size)] += compound_score
            elif compound_score > 0.25:
                sentiment_stats['positive sentiments'] += 1
                pos_counter += 1
                batch_mean_compound_pos[(documents_processed // batch_size)] += compound_score
            else:
                sentiment_stats['neutral sentiments'] += 1
                neu_counter += 1
                batch_mean_compound_neu[(documents_processed // batch_size)] += compound_score

            batch_mean_compound[(documents_processed // batch_size)] += compound_score
            

        batch_mean_compound[(documents_processed // batch_size)] /= batch_size # Gets the mean compound score of the batch

        if neg_counter > 0:
            batch_mean_compound_neg[(documents_processed // batch_size)] /= neg_counter
        if neu_counter > 0:
            batch_mean_compound_neu[(documents_processed // batch_size)] /= neu_counter
        if pos_counter > 0:
            batch_mean_compound_pos[(documents_processed // batch_size)] /= pos_counter

        documents_processed += len(batch)
        print(documents_processed)

    sentiment_stats['mean compound'] = mean(batch_mean_compound)
    sentiment_stats['mean negative compound'] = mean(batch_mean_compound_neg)
    sentiment_stats['mean neutral compound'] = mean(batch_mean_compound_neu)
    sentiment_stats['mean positive compound'] = mean(batch_mean_compound_pos)

    return sentiment_stats

def plot_sentiment_stats(sentiment_stats: dict, show_means = False, chart_type='pie') -> None:

    """
    Plots the given tweets their sentiment distribtuion on a bar chart.
    :param sentiment_stats: a dictionary containing the number of each sentiment type
    :param show_means: a bool dictating whether to show the sentiment counts or means
    """

    sentiment_types = list(sentiment_stats.keys())
    sentiment_counts = list(sentiment_stats.values())

    if not show_means:
        sentiment_types = sentiment_types[0:3]
        sentiment_counts = sentiment_counts[0:3]
    else:
        sentiment_types = sentiment_types[3:]
        sentiment_counts = sentiment_counts[3:]


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

