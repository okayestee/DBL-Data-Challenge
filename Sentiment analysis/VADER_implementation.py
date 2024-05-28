import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

analyzer = SentimentIntensityAnalyzer()

text = 'Christmas ruined'

with open('Sentiment analysis/sample', 'r', encoding='utf-8') as file:
    texts: list[str] = file.readlines()

for tweet in texts:
    scores = analyzer.polarity_scores(tweet)
    print(tweet)
    print(f'{scores}\n')