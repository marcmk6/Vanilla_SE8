import numpy as np
import pandas as pd
from math import log10
from scipy.sparse import csr_matrix
import ray
from os import cpu_count
from time import time
from memory_profiler import profile

from index_configuration import IndexConfiguration
from text_processing import process
from global_variable import COURSE_CORPUS, REUTERS_CORPUS

ray.init(num_cpus=cpu_count())


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


def __add_inverted_index__(doc_id: str, term: str):
    if term in inverted_index.keys():
        inverted_index[term].append(doc_id)
    else:
        inverted_index[term] = [doc_id]


def __sort_inverted_index_posting_lists__() -> dict:
    return {term: sorted(doc_id_list) for term, doc_id_list in inverted_index.items()}


def __update__(docid: str, term: str):
    __df_p1__(term)
    __tf_p1__(doc_id=docid, term=term)
    __add_inverted_index__(doc_id=docid, term=term)


def __process_row__(string: str, index_conf: IndexConfiguration, doc_id: str):
    terms = process(string=string, config=index_conf)
    all_terms.update(terms)
    for term in terms:
        __update__(docid=doc_id, term=term)


def __get_tf_idf_matrix__() -> csr_matrix:
    # tf_idf = {k: doc_id, v: {k: term, v: tf_idf value}}
    tf_idf_dict = {doc_id: {term: (log10(1 + tf) * log10(doc_count / raw_df[term] + 1))
                            for term, tf in v.items()}
                   for doc_id, v in raw_tf.items()}

    doc_ids = sorted(raw_tf.keys())
    terms = sorted(raw_df.keys())

    def __get_chunks__(lst, n):
        chunk_size = round(len(lst) / n)
        r = []
        for i in range(0, len(lst), chunk_size):
            r.append(lst[i:i + chunk_size])
        return r

    @ray.remote
    def __worker__(shared_tf_idf_dict, doc_ids_chunk, shape):
        tf_idf_matrix_piece = np.zeros((shape[0], shape[1]), dtype=np.float16)
        for row, doc_id in enumerate(doc_ids_chunk):
            terms_in_doc = raw_tf[doc_id].keys()
            for term in terms_in_doc:
                col = terms.index(term)
                tf_idf_matrix_piece[row, col] = shared_tf_idf_dict[doc_id][term]
        return tf_idf_matrix_piece

    doc_ids_chunks = __get_chunks__(doc_ids, cpu_count())

    tf_idf_dict_id = ray.put(tf_idf_dict)
    results = ray.get(
        [__worker__.remote(tf_idf_dict_id, doc_ids_chunks[i], (len(doc_ids_chunks[i]), len(terms))) for i in
         range(len(doc_ids_chunks))])
    tf_idf_matrix = np.concatenate(results, axis=0)
    # tf_idf_matrix = np.zeros((len(raw_tf.keys()), len(raw_df.keys())), dtype=np.float16)
    #
    # doc_ids = sorted(raw_tf.keys())
    # terms = sorted(raw_df.keys())
    #
    # for row, doc_id in enumerate(doc_ids):
    #     terms_in_doc = raw_tf[doc_id].keys()
    #     for term in terms_in_doc:
    #         col = terms.index(term)
    #         tf_idf_matrix[row, col] = tf_idf_dict[doc_id][term]
    # tf_idf_matrix.save('../index/wtf')
    return csr_matrix(tf_idf_matrix)


all_terms = set()
raw_df = {}  # k: term, v: df value
raw_tf = {}  # k: doc_id, v: {k: term, v: tf value}
inverted_index = {}  # k: term, v: doc_id list

corpus_path = COURSE_CORPUS
ic = IndexConfiguration(True, True, True)

start = time()
corpus_df = pd.read_csv(corpus_path, names=['doc_id', 'title', 'content'], dtype=str, na_filter=False, index_col=False)

doc_count = len(corpus_df.index)
corpus_df['to_be_processed'] = corpus_df['title'] + ' ' + corpus_df['content']
corpus_df.drop(columns=['title', 'content'], inplace=True)
corpus_df.apply(lambda row: __process_row__(doc_id=row['doc_id'], string=row['to_be_processed'], index_conf=ic), axis=1)
inverted_index = __sort_inverted_index_posting_lists__()

tfidf = __get_tf_idf_matrix__()

print('time:%s' % (time() - start))

pass
