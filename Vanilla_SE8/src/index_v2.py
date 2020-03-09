import numpy as np
import pandas as pd
from math import log10, floor
from scipy.sparse import csr_matrix
from os import cpu_count
from os.path import exists
from time import time
import ray
import pickle

from intermediate_class.index_configuration import IndexConfiguration
from intermediate_class.query_completion import QueryCompletion
from util.text_processing import process
from global_variable import COURSE_CORPUS, INDEX_DIR, INDEX_FILE_EXTENSION, QUERY_COMPLETION_FILE_EXTENSION, \
    BLM_THRESHOLD, REUTERS_CORPUS
from util.wildcard_handler import get_bigrams


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
        self.bigram_index = None  # mapping bigram to terms

    def build(self):
        """Build and save index"""
        ray.init(num_cpus=cpu_count())
        _index = __build_index__(corpus_path=self.__corpus__, index_conf=self.config)
        ray.shutdown()
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
        def __worker__(_tf_idf_dict, _terms, _raw_tf, doc_ids_chunk, shape):

            tf_idf_matrix_piece = np.zeros((shape[0], shape[1]), dtype=np.float16)

            for row, doc_id in enumerate(doc_ids_chunk):
                terms_in_doc = _raw_tf[doc_id].keys()
                for term in terms_in_doc:
                    col = _terms.index(term)
                    tf_idf_matrix_piece[row, col] = _tf_idf_dict[doc_id][term]
            return tf_idf_matrix_piece

        doc_ids = sorted(raw_tf.keys())
        terms = sorted(raw_df.keys())

        doc_ids_chunks = __get_chunks__(doc_ids, cpu_count())

        _tf_idf_dict_id = ray.put(tf_idf_dict)
        _terms_id = ray.put(terms)
        _raw_tf_id = ray.put(raw_tf)

        results = ray.get([
            __worker__.remote(
                _tf_idf_dict=_tf_idf_dict_id, _terms=_terms_id, _raw_tf=_raw_tf_id,
                doc_ids_chunk=doc_ids_chunks[i],
                shape=(len(doc_ids_chunks[i]), len(terms))
            ) for i in range(len(doc_ids_chunks))
        ])

        tf_idf_matrix = np.concatenate(results, axis=0)

        return csr_matrix(tf_idf_matrix)

    """
    Main logic of constructing index
    """
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

    # Building bigram language model
    """
    the model here is in the form of {term1: [other terms]}, other terms are sorted by frequency decreasingly
    'other terms' are refer to those that have frequency more than the threshold
    """
    _blm_path = INDEX_DIR + ('0' if corpus_path == COURSE_CORPUS else '1') + QUERY_COMPLETION_FILE_EXTENSION
    if not exists(_blm_path):
        __build_bigram_language_model__(blm_out_path=_blm_path, corpus_df=corpus_df)

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


def __build_bigram_language_model__(blm_out_path, corpus_df) -> None:
    # during construction: k: term1, v: {k: term2, v: count of appearing after term1}
    # at returning time, v should be stored in decreasing order by v.v, for every term
    # v should also contain only term2 with v.v larger than threshold
    #
    # TODO v can be just sorted list of terms?

    import nltk

    def update_bigram_model(term1: str, term2: str):
        if term1 in bigram_model.keys():
            if term2 in bigram_model[term1].keys():
                bigram_model[term1][term2] += 1
            else:
                bigram_model[term1][term2] = 1
        else:
            bigram_model[term1] = {term2: 1}

    def finalize_bigram_model():
        all_terms = list(bigram_model.keys())
        tmp = {term1: sorted([(term2, count) for term2, count in v.items() if count >= BLM_THRESHOLD], reverse=True,
                             key=lambda x: x[1]) for term1, v in bigram_model.items()}
        tmp = {term1: [tpl[0] for tpl in v] for term1, v in tmp.items() if v != []}
        return tmp, all_terms

    def process_row(row_str):
        sentence_tokens_list = [
            [y for y in x.split()
             if (y not in nltk.corpus.stopwords.words('english') and y.isalpha())]
            for x in row_str.lower().split('.') if x != '']

        for sentence_tokens in sentence_tokens_list:
            for i in range(0, len(sentence_tokens) - 1):
                update_bigram_model(sentence_tokens[i], sentence_tokens[i + 1])

    bigram_model = {}
    corpus_df['blm'] = corpus_df['title'] + '.' + corpus_df['content']
    corpus_df['blm'].apply(lambda row: process_row(row))
    corpus_df.drop(columns='blm', inplace=True)
    bigram_model, all_terms = finalize_bigram_model()
    query_completion = QueryCompletion(bigram_model=bigram_model, all_terms=all_terms)

    with open(blm_out_path, 'wb') as f:
        pickle.dump(query_completion, f)


if __name__ == '__main__':
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
