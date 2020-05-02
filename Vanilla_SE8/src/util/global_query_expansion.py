from nltk.corpus import wordnet as wn
from time import time


def expand_query_globally(query: str) -> str:
    # FIXME
    # start = time()
    tokens = query.split()
    new_query = []
    for token in tokens:
        new_query.append(token)
        new_query += list(get_synonyms(token))
    # print('expand_query_globally: %s seconds' % (time() - start))
    return ' '.join(new_query)


def get_synonyms(word: str) -> set:
    synonyms = set()
    for i, syn in enumerate(wn.synsets(word)):
        if i < 3:
            tmp = [e for e in syn.lemma_names() if e.isalpha()]
            tmp = tmp[:3]
            synonyms.update(tmp)
        else:
            break
    return synonyms - {word}


if __name__ == '__main__':
    l = ['oil platform explosion',
         'linear algebra']
    for q in l:
        print(expand_query_globally(q))
