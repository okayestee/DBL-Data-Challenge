import pymongo
from statistics import mean


client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['DBL_data']
collection = db['sentiment_analysis']

trunc_error_counter = 0

batch_mean_compound: list[float] = []

pos_counter = 0
neg_counter = 0
neu_counter = 0

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
            trunc_error_counter += 1

        compound_score = document.get('compound sentiment')

        if compound_score < -0.2:
            neg_counter += 1
        elif compound_score > 0.25:
            pos_counter += 1
        else:
            neu_counter += 1

        batch_mean_compound[(documents_processed // batch_size)] += compound_score

    batch_mean_compound[(documents_processed // batch_size)] /= batch_size # Gets the mean compound score of the batch

    documents_processed += len(batch)
    print(documents_processed)

print(f'truncation errors: {trunc_error_counter}')
print(f'negatives: {neg_counter}, neutrals: {neu_counter}, positives: {pos_counter}')

compound_mean = mean(batch_mean_compound)
print(f'mean compound score: {compound_mean}')