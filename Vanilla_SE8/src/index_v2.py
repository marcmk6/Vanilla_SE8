import numpy as np
import pandas as pd
from math import log10, floor
from scipy.sparse import csr_matrix
from os import cpu_count
from time import time
import ray
import pickle
from memory_profiler import profile

from index_configuration import IndexConfiguration
from text_processing import process
from global_variable import COURSE_CORPUS, REUTERS_CORPUS, INDEX_DIR, INDEX_FILE_EXTENSION
from spelling_correction import SpellingCorrection
from wildcard_handler import get_bigrams


class Index_v2:

    def __init__(self, corpus: str, index_conf: IndexConfiguration):
        self.__corpus__ = corpus
        self.__corpus_id__ = '0' if corpus == COURSE_CORPUS else '1'  # 0 for course corpus, 1 for Reuters
        self.config = index_conf

        # placeholders
        self.tf_idf_matrix = None  # csr_matrix
        self.inverted_index = None  # k: term, v: doc_id list
        self.terms = None  # set
        self.tf_over_corpus = None
        self.doc_ids = None
        self.bigram_index = None

    def build(self):
        """Build and save index"""
        _index = __build_index__(corpus_path=self.__corpus__, index_conf=self.config)
        self.tf_idf_matrix = _index[0]
        self.inverted_index = _index[1]
        self.terms = sorted(list(_index[2]))
        self.doc_ids = _index[3]
        self.tf_over_corpus = _index[4]
        self.bigram_index = _index[5]

        out_path = INDEX_DIR + str(self) + INDEX_FILE_EXTENSION
        with open(out_path, 'wb') as f:
            pickle.dump(self, f)

    @staticmethod
    def load(index_file: str):
        with open(index_file, 'rb') as f:
            index = pickle.load(f)
        return index

    def __str__(self):
        return '%s_%s' % (str(self.__corpus_id__), str(self.config))

    def get(self, term: str) -> list:
        """Get postings list by term"""
        if term in self.terms:
            return self.inverted_index[term]
        else:
            return []

    def get_total_term_frequency(self, term: str) -> int:
        """Get term frequency over the entire corpus"""
        return self.tf_over_corpus[term]


def __build_index__(corpus_path: str, index_conf: IndexConfiguration):
    def __df_p1__(term: str):
        if term in raw_df.keys():
            raw_df[term] += 1
        else:
            raw_df[term] = 1

    def __tf_p1__(doc_id: str, term: str):
        if doc_id in raw_tf.keys():
            term_tf_dict = raw_tf[doc_id]
            if term in term_tf_dict.keys():
                term_tf_dict[term] += 1
            else:
                term_tf_dict[term] = 1
        else:
            raw_tf[doc_id] = {term: 1}

    def __update_inverted_index__(doc_id: str, term: str):
        if term in inverted_index.keys():
            inverted_index[term].add(doc_id)
        else:
            inverted_index[term] = {doc_id}

    def __sort_inverted_index_posting_lists__() -> dict:
        return {term: sorted(list(doc_id_set)) for term, doc_id_set in inverted_index.items()}

    def __update_bigram_index__(term: str):
        if term not in all_terms and (term.isalpha() and len(term) >= 2):
            bigrams = get_bigrams(term)
            for bigram in bigrams:
                if bigram in bigram_index.keys():
                    bigram_index[bigram].add(term)
                else:
                    bigram_index[bigram] = {term}

    def __update_tf_over_corpus__(term: str):
        if term in tf_over_corpus.keys():
            tf_over_corpus[term] += 1
        else:
            tf_over_corpus[term] = 1

    def __update_index__(docid: str, term: str):
        __df_p1__(term)
        __tf_p1__(doc_id=docid, term=term)
        __update_inverted_index__(doc_id=docid, term=term)
        __update_bigram_index__(term)
        __update_tf_over_corpus__(term)

    def __process_row__(string: str, index_conf: IndexConfiguration, doc_id: str):
        terms = process(string=string, config=index_conf)
        for term in terms:
            __update_index__(docid=doc_id, term=term)
        all_terms.update(terms)

    def __get_tf_idf_matrix__() -> csr_matrix:

        # tf_idf = {k: doc_id, v: {k: term, v: tf_idf value}}
        tf_idf_dict = {doc_id: {term: (log10(1 + tf) * log10(doc_count / raw_df[term] + 1))
                                for term, tf in v.items()}
                       for doc_id, v in raw_tf.items()}

        doc_ids = sorted(raw_tf.keys())
        terms = sorted(raw_df.keys())

        def __get_chunks__(lst, n):
            chunk_size = floor(len(lst) / n)
            r = []
            ptr = 0
            for i in range(0, n - 1):
                r.append(lst[ptr:ptr + chunk_size])
                ptr += chunk_size
            r.append(lst[ptr:])
            return r

        @ray.remote
        def __worker__(shared_tf_idf_dict, shared_terms, doc_ids_chunk, shape):
            tf_idf_matrix_piece = np.zeros((shape[0], shape[1]), dtype=np.float16)
            for row, doc_id in enumerate(doc_ids_chunk):
                terms_in_doc = raw_tf[doc_id].keys()
                for term in terms_in_doc:
                    col = shared_terms.index(term)
                    tf_idf_matrix_piece[row, col] = shared_tf_idf_dict[doc_id][term]
            return tf_idf_matrix_piece

        doc_ids_chunks = __get_chunks__(doc_ids, cpu_count())

        ray.init(num_cpus=cpu_count())
        _tf_idf_dict_id = ray.put(tf_idf_dict)
        _terms_id = ray.put(terms)
        results = ray.get(
            [__worker__.remote(_tf_idf_dict_id, _terms_id, doc_ids_chunks[i],
                               (len(doc_ids_chunks[i]), len(terms))) for i in range(len(doc_ids_chunks))])
        ray.shutdown()

        tf_idf_matrix = np.concatenate(results, axis=0)

        return csr_matrix(tf_idf_matrix)

    # Main logic of constructing index
    all_terms = set()
    raw_df = {}  # k: term, v: df value
    raw_tf = {}  # k: doc_id, v: {k: term, v: tf value}
    inverted_index = {}  # k: term, v: doc_id list
    bigram_index = {}  # k: bigram, v: {terms}
    tf_over_corpus = {}  # k: term, v: term frequency over the entire corpus

    # Read corpus csv file
    corpus_df = \
        pd.read_csv(corpus_path, names=['doc_id', 'title', 'content'], dtype=str, na_filter=False, index_col=False)

    doc_count = len(corpus_df.index)
    corpus_df['to_be_processed'] = corpus_df['title'] + ' ' + corpus_df['content']
    corpus_df.drop(columns=['title', 'content'], inplace=True)

    # Constructing index by going through all rows
    corpus_df.apply(
        lambda row: __process_row__(doc_id=row['doc_id'], string=row['to_be_processed'], index_conf=index_conf), axis=1)
    inverted_index = __sort_inverted_index_posting_lists__()

    tf_idf_matrix_csr = __get_tf_idf_matrix__()

    return tf_idf_matrix_csr, inverted_index, all_terms, sorted(raw_tf.keys()), tf_over_corpus, bigram_index


def bigrams_2_terms(index: Index_v2, bigrams: set) -> set:
    terms_sets = []
    for bigram in bigrams:
        terms_sets.append(index.bigram_index[bigram])
    terms_set = set.intersection(*terms_sets)
    return terms_set


class _SearchResult:

    def __init__(self, doc_id_lst: list, correction: SpellingCorrection, result_scores: list):
        self.doc_id_lst = doc_id_lst
        self.correction = correction
        self.result_scores = result_scores


if __name__ == '__main__':
    # ray.init(num_cpus=cpu_count())
    corpus_path = REUTERS_CORPUS
    ic = IndexConfiguration(True, True, True)
    start = time()
    index = Index_v2(corpus=corpus_path, index_conf=ic)
    index.build()
    # with open('../index/new_terms.txt', 'w')as f:
    #     f.write(str(index.terms))
    print('time:%s' % (time() - start))

    # idx = NewIndex.load('../index/0_111.idx')

    pass
