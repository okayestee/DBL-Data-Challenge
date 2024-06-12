import os
import json
from tqdm import tqdm


def make_tweet_list(path: str)-> list[dict]: 
    """
    Reads the data from the file located at path
    :param path: path to the data to be loaded
    :returns: list of tweets
    """
    print('cleaning:'+ path)
   
    data = []
    with open(path, 'r', encoding='latin-1') as file:
        for line in file:
            try:
                if line[0] == '{':
                    data.append(json.loads(line))
            except json.decoder.JSONDecodeError as e:
                print('Error found at: ' + line)
                continue
        return data
    


def file_paths_list(path_to_data_folder: str) -> list[str]:
    '''
    Generates a list which contains the paths to all files in the given data folder
    :param path_to_data_folder: a string path to the data folder that we want to access
    :returns: a list of string paths to all individual files in the data folder
    '''

    file_path_list: list = list()
    file_names_list: list = os.listdir(path_to_data_folder)

    for file_name in file_names_list:
        file_path_list.append(f'{path_to_data_folder}/{file_name}')
      
    return file_path_list



def remove_variables(tweet: dict) -> dict:
    '''
    Cleans a given tweet by removing unnecessary variables.
    :param tweet: a dictionary representing a tweet
    :returns: a cleaned version of the tweet as a dictionary
    '''
    
    # All of the keys we want to clean
    variables_to_remove: list[str] = ["source", "location", "url", "protected", "contributors_enabled", "is_translator", "profile_background_color", "profile_background_image_url", "profile_background_image_url_https", "profile_background_tile", "profile_link_color", "profile_sidebar_border_color", "profile_sidebar_fill_color", "profile_text_color", "profile_use_background_image", "profile_image_url", "profile_image_url_https", "profile_banner_url", "default_profile", "default_profile_image", "following", "follow_request_sent", "notifications", "contributors", "urls" , "favorited", "retweeted"]

    keys_to_remove = list()

    for key in tweet:
        # Recurse through each tweet in the tweet object
        if type(tweet[key]) == dict:
            remove_variables(tweet[key])

        # Note all keys in the tweet that are also in the variables to remove list
        if key in variables_to_remove:                
            keys_to_remove.append(key)

    # Remove each key
    for key in keys_to_remove:
        del tweet[key]
    return tweet



def check_language(tweet: dict) -> bool:
    '''
    Checks whether a tweet is in english
    :param tweet: the tweet to be checked
    :returns: a boolean value representing whether the tweet is english
    '''

    if 'lang' in tweet:
        return tweet['lang'] == 'en'
    else:
        return False



def check_delete(tweet: dict) -> bool:
    '''
    Checks whether the tweet is deleted or not.
    :param tweet: the tweet to be checked
    :returns: a boolean value representing whether the tweet has been deleted
    '''

    if 'delete' in tweet:
        return True
    return False



def clean_all_files(path: str) -> None:
    '''
    Cleans all data and adds it to a big file
    :param path: path to the data folder
    '''

    file_path_list = file_paths_list(path)

    with tqdm(total=len(file_path_list), desc="Data cleaning", unit="documents") as pbar:
        with open(f'{path}/cleaned_data.json', 'a', encoding='latin-1') as new_file:
            for file_path in file_path_list:
                # For each line/tweet in the file, check if it should be included and write it into the cleaned data.
                lines: list = make_tweet_list(file_path)
                for line in lines:
                    if check_language(line):
                        new_file.write(json.dumps(remove_variables(line)) + '\n')
                pbar.update(1)

    print(f"All files cleaned and inserted into {path}/cleaned_data.json")

def check_media(tweet: dict) -> bool:
    """
    Checks whether a tweet or any tweets that are inside the tweet contain any media elements
    :param tweet: the tweet to be checked
    :returns: a boolean value representing whether the tweet has any media content
    """
    # Check the main tweet
    if 'media' in tweet:
        return True

    # Check all tweets inside the tweet
    for key in tweet:
        if type(tweet[key]) == dict:
            if check_media(tweet[key]):
                return True

    return False
