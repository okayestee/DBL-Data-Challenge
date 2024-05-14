import data_cleans as dc
import json

def languages(folder_path: str) -> dict:
    """
    Generates a dictionary that details how many tweets are in each language.
    :param folder_path: path to the data folder location.
    :returns: a dictionary with each language as key and the number of times it occurs in the dataset as value.
    """
    

    lang: dict = {'Missing': 0}
    file_path_list = dc.file_paths_list(folder_path)[0:-1]

    for file_path in file_path_list:

        lines = dc.make_tweet_list(file_path)
        for tweet in lines:
            if 'lang' in tweet:
                if tweet['lang'] not in lang:
                    lang[tweet['lang']] = 1
                else:
                    lang[tweet['lang']] += 1
            else:
                
                lang['Missing'] += 1
    return lang


  
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
                if count % 10000 == 0:
                    print(count)


    return count


def isAirlineInTweet(airline_name: str, tweet: dict) -> bool:
    
    if 'entities' in tweet:
        if 'user_mentions' in tweet['entities']:
            for mention_index in range(0, len(tweet['entities']['user_mentions'])):
                if 'screen_name' in tweet['entities']['user_mentions'][mention_index]:
                    if tweet['entities']['user_mentions'][mention_index]['screen_name'] == 'AmericanAir':
                        return True

    for key in tweet:
        if type(tweet[key]) == dict:
            if isAirlineInTweet(airline_name,tweet[key]):
                return True
    
    return False