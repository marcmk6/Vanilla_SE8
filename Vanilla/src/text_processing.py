import re
import nltk
from nltk.stem.porter import *
from index_configuration import IndexConfiguration

STEMMER = PorterStemmer()
HYPHEN_RE = re.compile("-")
PERIOD_RE = re.compile("(?<=[a-zA-Z])\.(?=[a-zA-Z])")


def rm_stop_words(tokens: list) -> list:
    return [t for t in tokens if t not in nltk.corpus.stopwords.words('english')]


def stem(tokens: list) -> list:
    return [STEMMER.stem(t) for t in tokens]


def normalize(s: str) -> str:
    s = HYPHEN_RE.sub(' ', s)  # low-cost -> low cost
    s = PERIOD_RE.sub('', s)  # U.S.A -> USA
    return s


def process(string: str, config: IndexConfiguration) -> list:
    stop_words_removal = config.stop_words_removal
    stemming = config.stemming
    normalization = config.normalization

    if normalization:
        string = normalize(string)

    tokens = nltk.word_tokenize(string)

    if stop_words_removal:
        tokens = rm_stop_words(tokens)

    if stemming:
        tokens = stem(tokens)

    # In case query contains only stopwords and they are removed
    if len(tokens) == 0:
        return ['']
    return tokens


# if __name__ == '__main__':
#     string = 'CSI_4107,CSI 4107 Information Retrieval and the Internet (3 units),Basic principles of Information Retriev' \
#              'al.  Indexing methods.  Query processing.  Linguistic aspects of Information Retrieval.  Agents and artifi' \
#              'cial intelligence approaches to Information Retrieval.  Relation of Information Retrieval to the World Wid' \
#              'e Web.  Search engines. Servers and clients.  Browser and server side programming for Information Retrieval.'
#
#     print(process(string))