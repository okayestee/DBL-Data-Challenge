from statistics import mean

def get_sentiment_stats(collection) -> dict:

    batch_mean_compound: list[float] = []

    sentiment_stats: dict[str, float] = dict()

    sentiment_stats['truncation errors'] = 0
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
        
        for document in batch:
            if document.get('truncated_error') == True:
                sentiment_stats['truncation errors'] += 1

            compound_score = document.get('compound sentiment')

            if compound_score < -0.2:
                sentiment_stats['negative sentiments'] += 1
            elif compound_score > 0.25:
                sentiment_stats['positive sentiments'] += 1
            else:
                sentiment_stats['neutral sentiments'] += 1

            batch_mean_compound[(documents_processed // batch_size)] += compound_score
            

        batch_mean_compound[(documents_processed // batch_size)] /= batch_size # Gets the mean compound score of the batch

        documents_processed += len(batch)
        print(documents_processed)

    sentiment_stats['mean compound'] = mean(batch_mean_compound)

    return sentiment_stats