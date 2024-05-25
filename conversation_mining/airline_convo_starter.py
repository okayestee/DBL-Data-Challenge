#setup mongoDB connection
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['DBL2'] #your database 
removed_dupl = db['removed_duplicates'] #collection where duplicates are removed

airline_ids =['56377143', '106062176', '18332190', '22536055', '124476322', '26223583', '2182373406', '38676903', '1542862735', '253340062', '45621423', '20626359', '218730857']

