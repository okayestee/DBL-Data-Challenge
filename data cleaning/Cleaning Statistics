import data_cleans as dc

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

print(media('data cleaning/../data'))