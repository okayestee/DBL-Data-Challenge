import pymongo
import re
import gc

def get_full_text(tweet):
    if tweet.get('truncated', True):
        return tweet.get('extended_tweet', {}).get('full_text', '')

    else:
        return tweet.get('text', '')
    

def batch_generator(collection, batch_size):
    tweets_cursor = collection.find({}).limit(batch_size)
    batch = list(get_full_text(tweet) for tweet in tweets_cursor)
    yield [clean(tweet_text) for tweet_text in batch]
    del batch
    gc.collect()




def clean(text):
    #remove @ppl, url
    output = re.sub(r'https://\S*','', text)
    output = re.sub(r'@\S*','',output)
    
    #remove \r, \n
    rep = r'|'.join((r'\r',r'\n'))
    output = re.sub(rep,'',output)

      #remove duplicated punctuation
    output = re.sub(r'([!()\-{};:,<>./?@#$%\^&*_~]){2,}', lambda x: x.group()[0], output)
    
    #remove extra space
    output = re.sub(r'\s+', ' ', output).strip()
    
    #remove string if string only contains punctuation
    if sum([i.isalpha() for i in output])== 0:
        output = ''
        
    #remove string if length<5
    if len(output.split()) < 5:
        output = ''

    output = output.lower()

    return output