from os import read
import re 
import fileinput
from typing import Iterator
import os


def remove_variables(tweet: str) -> str:
    '''
    Takes a tweet as a string and removes unnecessary variables
    :param tweet: string representation of tweet
    :returns: a string containing the cleaned version of the tweet
    '''

    variables_to_remove: list[str] = ["source", "location", "url", "protected", "utc_offset", "time_zone", "geo_enabled", "contributors_enabled", "is_translator", "profile_background_color", "profile_background_image_url", "profile_background_image_url_https", "profile_background_tile", "profile_link_color", "profile_sidebar_border_color", "profile_sidebar_fill_color", "profile_text_color", "profile_use_background_image", "profile_image_url", "profile_image_url_https", "profile_banner_url", "default_profile", "default_profile_image", "following", "follow_request_sent", "notifications"]

    # Removes the following variables from tweet string:
    for variable in variables_to_remove:
        regex = r'\"' + variable + r'\":[^,]+,'
        tweet = re.sub(regex, "", tweet)

    return tweet



def make_tweet_list(path: str)-> list[str]: 
    """
    Reads the data from the file located at path
    :param path: path to the data to be loaded
    :returns: list with a tweets
    """
    with open(path, 'r') as file:
       lines = file.readlines()
    return lines



def check_tweet(tweet: str) -> bool:
    """
    Checks whether the tweet should be kept by looking at media and language
    :param tweet: string representation of tweet
    :returns: A boolean value representing wether we want to keep the tweet in the data
    """
    media_regex = '\"media\":'
    en_lang_regex = '\"lang\":\"en\",'

    # Check whether the tweet has media
    if len(re.findall(media_regex, tweet)) > 0:
        return False
    # Check whether the tweet is in english
    elif len(re.findall(en_lang_regex, tweet)) > 0:
        return True
    else:
        return False
    



def clean_file(old_path: str, new_path:str) -> None:
    """
    Replaces the json file from old_path with a cleaned version of the file at new_path
    :param old_path: the path to the file that is to be cleaned
    :param new_path: the path where the new cleaned file will be generated
    """
    # Get each individual line from the file
    with open(old_path, 'r') as file:
       lines = file.readlines()

    # Write the cleaned version of the lines into the new file
    with open(new_path, 'a') as new_file:
        for number, line in enumerate(lines):

            # Checks if the tweet should be kept and if so adds it to the new file
            if check_tweet(line) == True:
                new_file.write(remove_variables(line))
            else:
                new_file.write('')
    


def make_file_iterator(path: str) -> Iterator:
    """
    Creates and returns an iterator that contains each line of a file
    :param path: the path to the file
    :returns: an iterator of each line in the file found at path
    """
    # Get each individual line from the file
    return fileinput.input(path)





def file_paths_list(path_to_data_folder: str) -> list:
    file_path_list: list = list()
    file_names_list: list = os.listdir(path_to_data_folder)
    for file_name in file_names_list:
        file_path_list.append(f'{path_to_data_folder}/{file_name}')  
    return file_path_list



def check_file(path: str) -> None:
    
    
    # Initialize a list for keeping track of duplicates  
    lines = make_tweet_list(path)
    
    with open(f'{path}/../tweet_variables', 'a') as new_file:
            # Checks if the tweet should be kept and if so adds it to the new file
            new_file.write('') 
    
    # go through all lines
    for tweet in lines:

        if check_tweet(tweet):
            # check if we've seen a duplicate of it before
            current_tweet_variables: list[str] = get_tweet_variables(tweet)

            if current_tweet_variables[0] == 'None' or current_tweet_variables[1] == 'None' or current_tweet_variables[2] == 'None':
                continue

            if current_tweet_variables not in make_tweet_list(f'{path}/../tweet_variables'):
                # if it's new, add it to the list we've seen
                store_tweet_variables(current_tweet_variables, path)

    return None

def store_tweet_variables(variables: list[str] ,path: str) -> None:
    with open(f'{path}/../tweet_variables', 'a') as new_file:
            # Checks if the tweet should be kept and if so adds it to the new file
            new_file.write(str(variables)+'\n') 

def get_tweet_variables(tweet: str) -> list[str]:

    text_regex = '\"text\":\"[^\"]+\",'
    user_name_regex = '\"name\":[^,]+,'
    timestamp_ms_regex = '\"timestamp_ms\":.+'

    current_tweet_variables: list[str] = [str(re.search(text_regex, tweet)), str(re.search(user_name_regex, tweet))]

    if len(re.findall(timestamp_ms_regex, tweet)) > 0:
        current_tweet_variables.append(str(re.findall(timestamp_ms_regex, tweet)[-1]))
    
    return current_tweet_variables



def clean_all_files(path: str) -> None:
    '''
    Cleans all data and adds it to a big file
    :param path: path to the data folder
    '''
    file_path_list = file_paths_list(path)

    with open(f'{path}/cleaned_data.json', 'a') as new_file:
        for file_path in file_path_list:
            lines = make_tweet_list(file_path)
            for line in lines:
                if check_tweet(line):
                    new_file.write(remove_variables(line))
    print(f"All files cleaned and inserted into {path}/airline_data.json")

