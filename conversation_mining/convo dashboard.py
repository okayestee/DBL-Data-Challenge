import airline_convo_starter


#then all these data folder must be at same level
data_folder_name: str = 'conversation' 
data_convo_mining_folder_name: str = 'convo_mining'
path: str = f'{data_convo_mining_folder_name}/../{data_folder_name}'
airline_convo_starter.clean_all_files(path)



