import csv
import pickle
from os.path import exists

import text_processing
from document import _Document
from index_configuration import IndexConfiguration
from global_variable import COURSE_CORPUS, REUTERS_CORPUS, TERMS_DIR, TERMS_FILE_EXTENSION


# Module 3 - Dictionary building
def build_vocabulary(corpus_id: int, config: IndexConfiguration) -> (list, list):
    corpus_path = COURSE_CORPUS if corpus_id == 0 else REUTERS_CORPUS
    terms_path = TERMS_DIR + str(corpus_id) + '_' + str(config) + TERMS_FILE_EXTENSION

    if exists(terms_path):
        terms = __load_terms__(terms_path)
        docs = []
    else:
        terms, docs = __build_vocabulary_v2__(corpus_path=corpus_path, config=config)
        __save_terms__(path=terms_path, terms=terms)
    return terms, docs


def __build_vocabulary_v2__(corpus_path, config: IndexConfiguration) -> (list, list):
    to_be_processed = ''
    docs = []
    with open(corpus_path, 'r') as corpus_file:
        reader = csv.reader(corpus_file)
        for row in reader:
            doc_id = row[0]
            title = row[1]
            content = row[2]
            to_be_processed += title.strip('\n') + ' ' + content.strip('\n') + ' '

            processed_title = ' '.join(text_processing.process(string=title.strip('\n'), config=config))
            processed_content = ' '.join(text_processing.process(string=content.strip('\n'), config=config))
            docs.append(_Document(doc_id=doc_id, title=processed_title, content=processed_content))

    return text_processing.get_terms(content=to_be_processed, config=config), docs


def __save_terms__(path: str, terms: list) -> None:
    with open(path, 'wb') as f:
        pickle.dump(terms, f)


def __load_terms__(file: str) -> list:
    with open(file, 'rb') as f:
        terms = pickle.load(f)
    return terms


def __build_vocabulary_v1__(corpus, config: IndexConfiguration) -> list:
    terms = set()
    with open(corpus, 'r') as corpus_file:
        reader = csv.reader(corpus_file)
        for row in reader:
            doc_id = row[0]
            title = row[1]
            content = row[2]

            # to_be_processed = [title, description]
            to_be_processed = [title.strip('\n') + content.strip('\n')]

            for e in to_be_processed:
                processed_tokens = text_processing.process(string=e, config=config)
                for term in processed_tokens:
                    terms.add(term)

    terms = terms - {''}  # Remove empty term
    terms = sorted(list(terms))
    return terms


if __name__ == "__main__":
    from time import time

    # start = time()
    # corpus = ['../corpus/course_corpus_full.csv', '../corpus/reuters_corpus.csv']
    # for c in corpus:
    #     build_vocabulary(corpus=c, config=IndexConfiguration(True, True, True))
    # print('time elapsed: %s' % (time() - start))

    # with open('../corpus/tmp.txt', 'w') as f:
    #     f.write(str(__load_terms__(REUTERS_TERMS)))
