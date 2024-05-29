import re

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