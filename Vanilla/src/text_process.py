import re
import nltk
from nltk.stem.porter import *

stemmer = PorterStemmer()
hyphen_re = re.compile("-")
period_re = re.compile("(?<=[a-zA-Z])\.(?=[a-zA-Z])")


def rm_stop_words(tokens):
    return [t for t in tokens if t not in nltk.corpus.stopwords.words('english')]


def stem(tokens):
    return [stemmer.stem(t) for t in tokens]


def normalize(s):
    s = hyphen_re.sub(' ', s)  # low-cost -> low cost
    s = period_re.sub('', s)  # U.S.A -> USA
    return s


def process(string, stop_words_removal, stemming, normalization):
    if normalization:
        string = normalize(string)
    tokens = nltk.word_tokenize(string)
    if stop_words_removal:
        tokens = rm_stop_words(tokens)
    if stemming:
        tokens = stem(tokens)
    return tokens
