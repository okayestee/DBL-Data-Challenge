from pymongo import MongoClient, IndexModel, ASCENDING
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pymongo.errors import BulkWriteError

from remove_duplicates import remove_duplicates
from remove_inconsistencies import remove_inconsistencies
from filter_replies import filter_replies
from user_starting_convo import user_convo_starters
from airline_starting_convo import airline_convo_starters
from airline_tree


def main():
    remove_duplicates()  # Call the function to remove duplicates
    remove_inconsistencies()  # Call the function to remove inconsistencies
    filter_replies() # Call the function to store all the reply tweets in a new collection
    user_convo_starters() # Call the function to store all user starter tweets in a new collection
    airline_convo_starters()  # Call the function to store all airline starter tweets in a new collection
    airline_trees()

if __name__ == "__main__":
    main()








