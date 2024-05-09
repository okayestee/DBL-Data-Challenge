import json

def read_file(path):
    with open(path, 'r') as file:
        new_file = json.load(file)
            
    

read_file('data cleaning/Test tweet 4')
