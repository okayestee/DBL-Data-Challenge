import json
import random
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

def select_random_lines(sample_size, file_path, number_of_lines_in_file):
    """
    Samples a specified amount of randomly chosen lines from a file.
    :param sample_size: the amount of lines to sampled
    :param file_path: the path to the file to be sampled.
    :param number_of_lines_in_file: the total number of lines in the file
    """

    sample: list[dict] = list()

    if sample_size > number_of_lines_in_file:
        raise ValueError("Number of lines to select is greater than the available lines in the JSON data.")
    
    for sample_number in range(0, sample_size):
        print(sample_number)
        random_line = random.randint(1, number_of_lines_in_file - 1)
        print(random_line)
        sample.append(select_line(file_path, random_line))
            
    return sample

def select_line(file_path, line_number) -> dict:
    with open(file_path, 'r') as file:
        for current_line_number, line in enumerate(file, start=1):
            if current_line_number == line_number:
                return json.loads(line)
        else:
            return dict() 

def get_text_from_tweets(tweets: list[dict]) -> list[str]:
    tweet_entries: list[str] = []

    for line in tweets:
        if 'extended_tweet' in line:
            extended_tweet: dict = line["extended_tweet"]
            tweet_entries.append(extended_tweet['full_text'])
        else:
            tweet_entries.append(line["text"])
    return tweet_entries

def count_lines(file_path: str):
    """
    Count the amont of lines in a given file
    :param file_path: path to the file to be counted
    :returns: the number of lines in the file
    """
    with open(file_path, 'r') as file:
        number_of_lines: int = sum(1 for _ in file)
        return number_of_lines

def save_sample(sample, file_name) -> None:
    with open(file_name, 'a', encoding='utf-8') as new_file:
        counter: int = 0
        for content in get_text_from_tweets(sample):
            counter += 1
            new_file.write(f'Tweet {counter}: {str(content)} \n')

def update_VADER(analyzer: SentimentIntensityAnalyzer):
    analyzer.lexicon['help'] = 0
    analyzer.lexicon['cancellation'] = -2.29
    analyzer.lexicon['cancelled'] = analyzer.lexicon['cancellation']
    analyzer.lexicon['canceled'] = analyzer.lexicon['cancellation']
    analyzer.lexicon['cancels'] = analyzer.lexicon['cancellation']
    analyzer.lexicon['cancelation'] = analyzer.lexicon['cancellation']
    analyzer.lexicon['cancelations'] = analyzer.lexicon['cancellation']
    analyzer.lexicon['cancellations'] = analyzer.lexicon['cancellation']
    analyzer.lexicon['long'] = -1.06
    analyzer.lexicon['transfer'] = 0.13
    analyzer.lexicon['transferred'] = analyzer.lexicon['transfer']
    analyzer.lexicon['nonstop'] = 0.06
    analyzer.lexicon['non-stop'] = analyzer.lexicon['nonstop']
    analyzer.lexicon['non stop'] = analyzer.lexicon['non-stop']
    analyzer.lexicon['direct'] = 0.45
    analyzer.lexicon['directly'] = analyzer.lexicon['direct']
    analyzer.lexicon['over-booked'] = -1.91
    analyzer.lexicon['overbooked'] = analyzer.lexicon['over-booked']
    analyzer.lexicon['over booked'] = analyzer.lexicon['over-booked']
    analyzer.lexicon['over books'] = analyzer.lexicon['over-booked']
    analyzer.lexicon['overbooks'] = analyzer.lexicon['over-booked']
    analyzer.lexicon['over-books'] = analyzer.lexicon['over-booked']
    analyzer.lexicon['offload'] = -1.63
    analyzer.lexicon['offloads'] = analyzer.lexicon['offload']
    analyzer.lexicon['offloaded'] = analyzer.lexicon['offload']
    analyzer.lexicon['off-load'] = analyzer.lexicon['offload']
    analyzer.lexicon['off-loaded'] = analyzer.lexicon['offload']
    analyzer.lexicon['off-loads'] = analyzer.lexicon['offload']
    analyzer.lexicon['problem'] = -2.24
    analyzer.lexicon['problems'] = analyzer.lexicon['problem']
    analyzer.lexicon['on time'] = 1.69
    analyzer.lexicon['ontime'] = analyzer.lexicon['on time']
    analyzer.lexicon['on-time'] = analyzer.lexicon['on time']
    analyzer.lexicon['terminal'] = 0.03

    return analyzer

# To get a random sample from the cleaned data text file, fill in the relevant paths and run the following code:
sample = select_random_lines(500, "PATH TO CLEANED DATA FILE", count_lines("PATH TO CLEANED DATA FILE"))

save_sample(sample, 'NEW LOCATION FOR SAMPLE')
