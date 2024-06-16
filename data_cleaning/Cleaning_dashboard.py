import data_cleans

# Make sure that all of your data is in one data folder
# The data folder must be at the same level (have same parent folder) as the folder this dashboard is in 

# Fill in the name of your data folder here:
data_folder_name: str = 'data'

# Fill in the name of the parent folder of this file
data_cleaning_folder_name: str = 'data_cleaning' # 'data cleaning' by default

# Once folder_name contains the correct name
# Run this file (Will take a while)
# Once it has finished running you should see a print message in your terminal
# The new file with the cleaned data will appear in the same folder as the data as 'airline_data'


# DO NOT TOUCH THIS
path: str = f'DBL-Data-Challenge/{data_cleaning_folder_name}/../{data_folder_name}'
data_cleans.clean_all_files(path)