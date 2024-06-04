import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['DBL_data']
collection = db['User_convos']

user_trees = list(collection.find({}))

def get_full_text(tweet):
    if tweet.get('truncated', True):
        return tweet.get('extended_tweet', {}).get('full_text', '')
    else:
        return tweet.get('text', '')
    
def get_replies(tree_data: dict) -> list:

    replies: list = []

    for reply_index in range(0, len(tree_data['children'])):
        replies.append(get_reply_by_index(tree_data, reply_index))


    return replies


def get_reply_by_index(tree: dict, reply_index: int):

    if 'children' in tree:

        reply_key = list(tree['children'][reply_index].keys())[0]
        reply = tree['children'][0][reply_key]  
    else:
        reply = {}

    return reply

    
def get_convo(tree_doc: dict):

    original_tweet = tree_doc['tree_data']
    original_tweet_text = get_full_text(original_tweet['data'])

    convo: list = [original_tweet_text]
    height = 0
    next_reply = get_reply_by_index(original_tweet, 0)

    while len(next_reply) != 0:
        convo.append(get_full_text(next_reply['data']))
        next_reply = get_reply_by_index(next_reply, 0)
    
    if len(convo) >= 3:
        return convo
    else:
        return None

for tree in user_trees:
    print(get_convo(tree))


