#!/usr/bin/env python

"""Clean comment text for easier parsing."""

from __future__ import print_function

import re
import string
import argparse
import sys
import json

__author__ = ""
__email__ = ""

# Some useful data.
_CONTRACTIONS = {
    "tis": "'tis",
    "aint": "ain't",
    "amnt": "amn't",
    "arent": "aren't",
    "cant": "can't",
    "couldve": "could've",
    "couldnt": "couldn't",
    "didnt": "didn't",
    "doesnt": "doesn't",
    "dont": "don't",
    "hadnt": "hadn't",
    "hasnt": "hasn't",
    "havent": "haven't",
    "hed": "he'd",
    "hell": "he'll",
    "hes": "he's",
    "howd": "how'd",
    "howll": "how'll",
    "hows": "how's",
    "id": "i'd",
    "ill": "i'll",
    "im": "i'm",
    "ive": "i've",
    "isnt": "isn't",
    "itd": "it'd",
    "itll": "it'll",
    "its": "it's",
    "mightnt": "mightn't",
    "mightve": "might've",
    "mustnt": "mustn't",
    "mustve": "must've",
    "neednt": "needn't",
    "oclock": "o'clock",
    "ol": "'ol",
    "oughtnt": "oughtn't",
    "shant": "shan't",
    "shed": "she'd",
    "shell": "she'll",
    "shes": "she's",
    "shouldve": "should've",
    "shouldnt": "shouldn't",
    "somebodys": "somebody's",
    "someones": "someone's",
    "somethings": "something's",
    "thatll": "that'll",
    "thats": "that's",
    "thatd": "that'd",
    "thered": "there'd",
    "therere": "there're",
    "theres": "there's",
    "theyd": "they'd",
    "theyll": "they'll",
    "theyre": "they're",
    "theyve": "they've",
    "wasnt": "wasn't",
    "wed": "we'd",
    "wedve": "wed've",
    "well": "we'll",
    "were": "we're",
    "weve": "we've",
    "werent": "weren't",
    "whatd": "what'd",
    "whatll": "what'll",
    "whatre": "what're",
    "whats": "what's",
    "whatve": "what've",
    "whens": "when's",
    "whered": "where'd",
    "wheres": "where's",
    "whereve": "where've",
    "whod": "who'd",
    "whodve": "whod've",
    "wholl": "who'll",
    "whore": "who're",
    "whos": "who's",
    "whove": "who've",
    "whyd": "why'd",
    "whyre": "why're",
    "whys": "why's",
    "wont": "won't",
    "wouldve": "would've",
    "wouldnt": "wouldn't",
    "yall": "y'all",
    "youd": "you'd",
    "youll": "you'll",
    "youre": "you're",
    "youve": "you've"
}

# You may need to write regular expressions.

def sanitize(sentence):
    """Do parse the text in variable "text" according to the spec, and return
    a LIST containing FOUR strings 
    1. The parsed text.
    2. The unigrams
    3. The bigrams
    4. The trigrams
    """
    # YOUR CODE GOES BELOW:
    
    #sentence = "I'm afraid I can't explain myself, sir. Because I am not myself, you see?"
    punct = string.punctuation
    sentence = sentence.lower()
    sentence = re.sub(r'https?:\/\/.*[\r\n]*', '', sentence)
    sentence = re.sub(r'\n', ' ', sentence)
    sentence = re.sub(r'\t', ' ', sentence)
    sentence = " " + sentence
    sentence = re.sub(r'( [%s_]+)(\w+)' %punct, r" \2", sentence)
    splits = re.findall(r"((\w+[%s]\w+)|[,.!?:;]|(\w+))" %punct, sentence)
    parse = []
    for i in splits:
    	parse.append(i[0])
    parses = ' '.join(parse)
    splits = [x for x in parse if x not in list(punct)]
    unigram = " ".join(splits)

    bigrams = ""

    for i in range(len(parse)):
    	if (i+1<len(parse)):
    		bigrams = bigrams + parse[i] + '_' + parse[i+1] + " "
	        
    bigrams = re.sub(r'[%s]_[\w%s]+|[\w%s]+_[%s]' %(punct, punct, punct, punct) , "" , bigrams)
    bigrams = re.sub(r' +'," ", bigrams)
    higrams = bigrams.rstrip()
    bigrams = higrams
	#sys.stdout.write(bigrams)

    trigrams = ""
    for i in range(len(parse)):
        if (i+2<len(parse)):
            trigrams = trigrams + parse[i] + '_' + parse[i+1] + "_" + parse[i+2] + " "
    trigrams = re.sub(r'[\w%(x)s]+_[%(x)s]_[%(x)s\w]+|[%(x)s]_[\w+%(x)s]+|[\w%(x)s]+_[%(x)s]' %{"x": punct} , "" ,trigrams)
    trigrams = re.sub(r' +'," ", trigrams)
    trigrams = trigrams.rstrip()


    #sys.stdout.write(parses)
    #if len(parse) >= 5:
    	#sys.stdout.write(parse[5])
    output = [unigram, bigrams, trigrams]
    return output
    #return [parsed_text, unigrams, bigrams, trigrams]


#method 1
#sentence = "I can eat less meat, and save a tiny fraction of the water that goes into one cow. \n\nOr I can save all the water that goes into superfluous flushing of toilets, usually at least twice per day, every day. Meat may be the biggest use of water that \"We\" are responsible for, but my toilet is something \"I\" am responsible for directly, and that's why I mentioned it. \n\nThanks for the wild left turn though, that's definitely a useful way to get your vegan thing across to people."
#splits = sentence.split(" ")
#splits = [x for x in splits if x != ""]
#punct = string.punctuation
#newb = []
#for i in splits:
#    temp = re.findall(r"(\w+[%s]\w+)|[,.!?:;]|(\w+)" %punct,i )
#    newb.append(temp)
#parse_text = " ".join(splits)

#method 2



#splits = sentence.split(" ")
#splits = [x for x in splits if x != ""]

if __name__ == "__main__":
    # This is the Python main function.
    # You should be able to run
    # python cleantext.py <filename>
    # and this "main" function will open the file,
    # read it line by line, extract the proper value from the JSON,
    # pass to "sanitize" and print the result as a list.
#sentence = "I'm afraid I can't explain myself, sir. Because I am not myself, you see?"
#sanitize(sentence)

    with open(sys.argv[1], 'r') as f:
        for line in f:
            distros_dict = json.loads(line)
            sys.stdout.write("\n"+sanitize(distros_dict['body'])[0])
            sys.stdout.write("\n"+sanitize(distros_dict['body'])[1])
            sys.stdout.write("\n"+sanitize(distros_dict['body'])[2])
            sys.stdout.write("\n"+sanitize(distros_dict['body'])[3])
    
    # YOUR CODE GOES BELOW.
