import data_cleans
from conversation_mining import remove_duplicates, remove_inconsistencies
import make_collection
import pymongo


# Make sure that all of your data is in one data folder
# The data folder must be at the same level (have same parent folder) as the folder this dashboard is in 

# Fill in the name of your data folder here:
data_folder_name: str = 'data'

# Fill in the name of the parent folder of this file
data_cleaning_folder_name: str = 'data cleaning' # 'data cleaning' by default

# Once folder_name contains the correct name
# Run this file (Will take a while)
# Once it has finished running you should see a print message in your terminal
# The new file with the cleaned data will appear in the same folder as the data as 'airline_data'


# Fill in the name of you database in mongoDB
db = 'Airline_data'

# DO NOT TOUCH THIS
path: str = f'DBL-Data-Challenge/{data_folder_name}'
data_cleans.clean_all_files(path)


#make a collection in mongoDB
make_collection(path, db)

remove_duplicates.remove_duplicates(db)
remove_inconsistencies.remove_inconsistencies(db)