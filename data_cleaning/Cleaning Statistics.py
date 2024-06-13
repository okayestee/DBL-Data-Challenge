import data_cleans as dc
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import threading
import matplotlib.pyplot as plt

def validate_date(date_str: str) -> datetime:
    """
    Validates the date string and converts it to a datetime object.
    :param date_str: The date string in 'YYYY-MM-DD' format.
    :returns: A datetime object corresponding to the date string.
    :raises ValueError: If the date is invalid.
    """
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError as e:
        raise ValueError(f"Invalid date: {date_str}. Please use the 'YYYY-MM-DD' format and ensure the date is valid.") from e

def process_file(file_path: str, start_date: datetime, end_date: datetime, output_dict: dict) -> None:
    """
    Process a single file and update the language counts within the specified timeframe.
    :param file_path: Path to the file to process.
    :param start_date: Start date of the timeframe.
    :param end_date: End date of the timeframe.
    :param output_dict: Dictionary to store language counts.
    """
    try:
        lines = dc.make_tweet_list(file_path)
    except PermissionError as e:
        print(f"Permission denied for file: {file_path}. Skipping this file.")
        return
    
    local_lang_dict = {}

    for tweet in lines:
        if 'created_at' in tweet:
            try:
                tweet_date = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
            except ValueError as e:
                print(f"Skipping tweet with invalid date format: {tweet['created_at']}")
                continue
            
            if start_date <= tweet_date <= end_date:
                lang_code = tweet.get('lang', 'Missing')
                local_lang_dict[lang_code] = local_lang_dict.get(lang_code, 0) + 1

    # Update shared dictionary using thread lock
    with threading.Lock():
        for lang, count in local_lang_dict.items():
            output_dict[lang] = output_dict.get(lang, 0) + count

def languages_in_timeframe_threadpool(folder_path: str, start_date_str: str, end_date_str: str, batch_size: int = 8) -> dict:
    """
    Generates a dictionary that details how many tweets are in each language within a given timeframe using ThreadPoolExecutor.
    :param folder_path: Path to the data folder location.
    :param start_date_str: Start date of the timeframe in 'YYYY-MM-DD' format.
    :param end_date_str: End date of the timeframe in 'YYYY-MM-DD' format.
    :param batch_size: Number of files to process per batch.
    :returns: A dictionary with each language as key and the number of times it occurs in the dataset as value.
    """
    lang: dict = {'Missing': 0}
    file_path_list = dc.file_paths_list(folder_path)

    start_date = validate_date(start_date_str)
    end_date = validate_date(end_date_str)

    # Ensure the start date is before the end date
    if start_date > end_date:
        raise ValueError("Start date must be before end date.")

    # Dictionary to store language counts
    lang_dict = {}
    
    # Use ThreadPoolExecutor with a fixed number of threads
    with ThreadPoolExecutor(max_workers=batch_size) as executor:
        futures = []
        for file_path in file_path_list:
            futures.append(executor.submit(process_file, file_path, start_date, end_date, lang_dict))
        
        # Use tqdm to create a progress bar for processing files
        with tqdm(total=len(file_path_list), desc="Processing files", unit="file") as pbar:
            for future in as_completed(futures):
                try:
                    future.result()  # Retrieve result to handle exceptions if any
                    pbar.update(1)  # Update progress bar for each completed file
                except Exception as e:
                    print(f"Error processing file: {e}")

    return lang_dict

def visualize_languages(lang_dict: dict) -> None:
    """
    Visualizes the language distribution as a pie chart with English and non-English tweets.
    :param lang_dict: dictionary with each language as key and the number of times it occurs in the dataset as value.
    """
    english_count = lang_dict.get('en', 0)
    non_english_count = sum(count for lang, count in lang_dict.items() if lang != 'en' and lang != 'Missing')

    # Avoid division by zero and invalid value issues
    if english_count == 0 and non_english_count == 0:
        print("No tweets found in the given timeframe.")
        return

    total_tweets = sum(lang_dict.values())
    english_percentage = (english_count / total_tweets) * 100
    non_english_percentage = (non_english_count / total_tweets) * 100

    labels = [f'English\n({english_count})', f'Non-English\n({non_english_count})']
    sizes = [english_count, non_english_count]
    colors = ['#156082', '#e97132']  # Custom hex color codes for blue and orange

    plt.figure(figsize=(8, 8))
    patches, texts, autotexts = plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=140)
    plt.title('Distribution of English vs Non-English Tweets\n'
              f'Total Tweets: {total_tweets}\n'
              f'English: {english_count} ({english_percentage:.1f}%)\n'
              f'Non-English: {non_english_count} ({non_english_percentage:.1f}%)')

    # Set text color to white
    for text in texts + autotexts:
        text.set_color('white')

    plt.show()
# Example usage
folder_path = "/Users/20235050/Downloads/DBL_Y1/DBL gitlab/DBL-Data-Challenge/data"
start_date_str = "2020-01-01"
end_date_str = "2020-01-03"

try:
    lang_dict = languages_in_timeframe_threadpool(folder_path, start_date_str, end_date_str, batch_size=8)
    visualize_languages(lang_dict)
except ValueError as e:
    print(e)


def media(folder_path: str) -> tuple:
    """
    Counts the amount of tweets in the dataset and the number of tweets with media files.
    :param folder_path: path to the data folder location.
    :returns: the total number of tweets followed by the number of tweets that contain a media file.
    """
    
    media_count: int = 0
    total_count: int = 0
    file_path_list = dc.file_paths_list(folder_path)[0:-1]

    for file_path in file_path_list:
        lines = dc.make_tweet_list(file_path)
        for tweet in lines:
            total_count += 1
            if dc.check_media(tweet):
                media_count += 1
                
    return (total_count, media_count)


def AirlineTweets(airline_name:str, path: str) -> int:
    """
    Counts the amount of tweets containing mentions of a chosen airline.
    :param airline_name: name of the chosen airline.
    :path: path to the file containing the tweets
    :returns: the number of tweets mentioning the airline.
    """

    count = 0

    with open(path, 'r') as file:

        for line in file:
            tweet = json.loads(line)

            if isAirlineInTweet(airline_name, tweet):
                count += 1

    return count


def isAirlineInTweet(airline_name: str, tweet: dict) -> bool:
    """
    Checks whether an airline has been mentioned somewhere in a tweet.
    :param airline_name: the name of the airline we want to find a mention of
    :param tweet: a dictionary representation of the tweet we want to check
    :returns: a boolean value representing whether we have found a mention of the airline or not
    """
    # Check if the tweet has entities data
    if 'entities' in tweet:

        # Iterate through all of the mentions
        if 'user_mentions' in tweet['entities']:
            for mention_index in range(0, len(tweet['entities']['user_mentions'])):

                # Check whether the screen name of the mention is equal to the name of the desired airline
                if 'screen_name' in tweet['entities']['user_mentions'][mention_index]:
                    if tweet['entities']['user_mentions'][mention_index]['screen_name'] == airline_name:
                        return True

    # Recurse through every tweet inside the original tweet
    for key in tweet:
        if type(tweet[key]) == dict:
            if isAirlineInTweet(airline_name,tweet[key]):
                return True
    
    return False

