import re
import nltk
from nltk.stem.porter import *

STEMMER = PorterStemmer()
hyphen_re = re.compile("-")
period_re = re.compile("(?<=[a-zA-Z])\.(?=[a-zA-Z])")


def rm_stop_words(tokens: list) -> list:
    return [t for t in tokens if t not in nltk.corpus.stopwords.words('english')]


def stem(tokens: list) -> list:
    return [STEMMER.stem(t) for t in tokens]


def normalize(s: str) -> str:
    s = hyphen_re.sub(' ', s)  # low-cost -> low cost
    s = period_re.sub('', s)  # U.S.A -> USA
    return s


def process(string: str, stop_words_removal=True, stemming=True, normalization=True) -> list:
    if normalization:
        string = normalize(string)
    tokens = nltk.word_tokenize(string)
    if stop_words_removal:
        tokens = rm_stop_words(tokens)
    if stemming:
        tokens = stem(tokens)
    return tokens
