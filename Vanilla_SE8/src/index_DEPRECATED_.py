import gc
import csv
import pickle
import logging
import numpy as np
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfTransformer
from memory_profiler import profile

from dictionary_DEPRECATED import build_vocabulary
import text_processing
from index_configuration import IndexConfiguration
from wildcard_handler import get_bigrams, bigram_term_matched
from spelling_correction import SpellingCorrection
import global_variable

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')


class Index:
    def old_method(self, corpus_path, config):
        self.terms, docs = build_vocabulary(self.corpus_id, self.config)
        logging.info('corpus_id: %s, %s, terms size: %s' % (self.corpus_id, config, len(self.terms)))

        id_lst = []
        for doc in docs:
            id_lst.append(doc.doc_id)

        tf, df = self.__construct_index__(self.terms, docs)
        self.df_dict = df
        self.docid_tf_dict = tf
        self.doc_ids = id_lst
        self.doc_count = len(self.doc_ids)
        logging.info('%s, %s, Start building tfidf' % (corpus_path, config))
        self.tf_idf_matrix = self.__get_tf_idf_matrix__(tf_matrix=self.__get_tf_matrix__(df_dict=df),
                                                        df_matrix_row=self.__get_df_matrix_row__(df_dict=df),
                                                        doc_count=self.doc_count)
        gc.collect()
        logging.info('%s, %s, tfidf built, size: %s' % (corpus_path, config, self.tf_idf_matrix.shape))

    def __init__(self, config=None, corpus_path=None):
        self.config = config

        self.corpus_id = 0 if 'course' in corpus_path else 1

        # TODO remove
        self.new = False

        def new_method(corpus_path):
            tmp = self.__tmp__(corpus_path)
            self.terms = tmp[2]
            logging.info('corpus_id: %s, %s, terms size: %s' % (self.corpus_id, config, len(self.terms)))
            self.docid_tf_dict = tmp[0]
            self.doc_ids = tmp[3]
            self.doc_count = len(self.doc_ids)
            logging.info('%s, %s, Start building tfidf' % (corpus_path, config))
            self.tf_idf_matrix = tmp[1]
            logging.info('%s, %s, tfidf built' % (corpus_path, config))
            self.tf_over_corpus_dct = tmp[4]

        if self.new:
            new_method(corpus_path)
        else:
            self.old_method(corpus_path, config)
        self.secondary_index = self.__build_secondary_index__()

    def __construct_index__(self, terms, documents) -> (dict, dict):
        df_dict = {}  # k: term, v: df
        docid_tf_dict = {}  # k: term, v: postings (dict:{doc_id, tf})

        for term in terms:
            df_dict[term] = 0
            docid_tf_dict[term] = {}

        for doc in documents:
            doc_id = doc.doc_id
            search_field = doc.title + ' ' + doc.content
            for term in terms:
                count = search_field.count(term)
                if count > 0:
                    df_dict[term] += 1
                    docid_tf_dict[term][doc_id] = count

        # Sort posting lists
        for term in terms:
            docid_tf_dict[term] = dict(sorted(docid_tf_dict[term].items()))

        return docid_tf_dict, df_dict

    @staticmethod
    def __get_tf_idf_matrix__(tf_matrix: np.ndarray, df_matrix_row: np.ndarray, doc_count: int) -> csr_matrix:
        """
        Calculate tf-idf
        :param tf_matrix: term frequency, dimension (d,v)
        :param df_matrix_row: document frequency, dimension (d,v)
        :param doc_count: number of documents
        :return: tf-idf in csr_matrix format, dimension (d,v)
        """
        doc_count = tf_matrix.shape[0]

        df_matrix_row = (np.log10(doc_count / (df_matrix_row + 1))).astype(np.float16)

        def mtply(tf_matrix_row):
            return df_matrix_row * tf_matrix_row

        # df_matrix = csr_matrix(np.tile(df_matrix_row, reps=[doc_count, 1]), dtype=np.float16)

        tf_matrix = (np.log10(1 + tf_matrix)).astype(np.float16)

        tmp = csr_matrix((np.apply_along_axis(mtply, 1, tf_matrix)).astype(np.float16))
        return tmp

    def __get_df_matrix_row__(self, df_dict) -> np.ndarray:
        """
        Raw document frequency, one row
        :return: csr_matrix (1, v)
        """
        df_lst = list(self.df_dict.values())
        tmp = df_lst
        # There are 21578 documents in Reuters corpus, int16 is therefore sufficient.
        tmp = np.asarray(tmp, dtype=np.int16)
        return tmp

    def __get_tf_matrix__(self, df_dict) -> np.ndarray:
        """
        Calculate raw term frequency
        :return:  csr_matrix (d,v)
        """

        def _get_doc_idx(doc_id: str) -> int:
            return self.doc_ids.index(doc_id)

        lst = []
        terms = self.df_dict.keys()
        for term in terms:
            col = [0] * self.doc_count
            tmp = self.docid_tf_dict[term]
            for doc_id, tf in tmp.items():
                idx = _get_doc_idx(doc_id)
                col[idx] = tf
            lst.append(col)

        tf_matrix = np.asarray(lst, dtype=np.int32).transpose()
        gc.collect()
        # tf_matrix = csr_matrix(tf_matrix)
        return tf_matrix

    def get(self, term: str) -> list:
        if term in self.docid_tf_dict.keys():
            # FIXME
            if not self.new:
                return list(self.docid_tf_dict[term].keys())
            else:
                return list(self.docid_tf_dict[term])
        else:
            return []

    def save(self) -> None:
        logging.info('Start saving index %s' % (str(self)))
        self.__save_pickle__()
        logging.info('Index %s saved.' % (str(self)))

    def __save_pickle__(self) -> None:
        """
        Save the Index object by serializing
        :return: None
        """
        out = global_variable.INDEX_DIR + str(self) + global_variable.INDEX_FILE_EXTENSION
        with open(out, 'wb') as f:
            pickle.dump(self, f)

    @staticmethod
    def load(index_file):
        return Index.__load_pickle__(index_file)

    @staticmethod
    def __load_pickle__(index_file):
        """
        Load the Index object
        :param index_file: path to the index file
        :return: index
        """
        with open(index_file, 'rb') as f:
            index = pickle.load(f)
        return index

    def __str__(self):
        return '%s_%s' % (str(self.corpus_id), str(self.config))

    def __build_secondary_index__(self):
        # INPLEMENTED
        """Build bigram index"""

        secondary_index = {}

        # Get all possible bigrams from terms
        all_bigrams = set()
        for term in self.terms:
            if term.isalpha() and len(term) >= 2:
                all_bigrams = all_bigrams.union(get_bigrams(term))

        for bigram in list(all_bigrams):
            matched_terms = []
            for term in self.terms:
                if bigram_term_matched(bigram=bigram, term=term):
                    matched_terms.append(term)
            secondary_index[bigram] = sorted(matched_terms)  # FIXME: sort or not?

        return secondary_index

    def get_total_term_frequency(self, term: str) -> int:
        # FIXME remove
        # IMPLEMENTED
        if not self.new:
            dct = self.docid_tf_dict[term]
            return sum(list(dct.values()))
        else:
            return self.tf_over_corpus_dct[term]

    def __tmp__(self, corpus_path: str) -> (dict, csr_matrix, list, list, dict):

        # Read corpus
        corpus = []
        doc_id_list = []
        with open(corpus_path, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                doc_id = row[0]
                title = row[1]
                content = row[2]
                doc_id_list.append(doc_id)
                corpus.append(title + ' ' + content)

        def preprocessor(s):
            return ' '.join(text_processing.process(s, config=self.config))

        pipe = Pipeline([('count', CountVectorizer(preprocessor=preprocessor, dtype=np.int32)),
                         ('tfidf', TfidfTransformer(use_idf=True, smooth_idf=True, sublinear_tf=True))]).fit(corpus)

        terms = pipe['count'].get_feature_names()

        # total term frequency over corpus
        tf_over_corpus = [sum(e) for e in pipe['count'].transform(corpus).transpose().toarray().tolist()]
        tf_over_corpus_dct = {}
        for i in range(0, len(terms)):
            tf_over_corpus_dct[terms[i]] = tf_over_corpus[i]

        # tf-idf matrix
        tfidf_matrix = (pipe.transform(corpus)).astype(np.float16)
        tfidf_matrix_T_list = tfidf_matrix.transpose().toarray().tolist()

        # inverted index
        inverted_index = {}
        for i, term in enumerate(terms):
            postings = []
            tfidf_v_lst = tfidf_matrix_T_list[i]
            for tfidf_v, doc_id in zip(tfidf_v_lst, doc_id_list):
                if tfidf_v > 0:
                    postings.append(doc_id)
            inverted_index[term] = postings

        return inverted_index, tfidf_matrix, terms, doc_id_list, tf_over_corpus_dct


def bigrams_2_terms(index: Index, bigrams: set) -> set:
    # IMPLEMENTED
    terms = []
    for bigram in bigrams:
        terms.append(set(index.secondary_index[bigram]))
    r = set.intersection(*terms)
    return r


class _SearchResult:

    def __init__(self, doc_id_lst: list, correction: SpellingCorrection, result_scores: list):
        self.doc_id_lst = doc_id_lst
        self.correction = correction
        self.result_scores = result_scores


if __name__ == '__main__':
    index = Index(corpus_path='../corpus/course_corpus.csv', config=IndexConfiguration(True, True, True))
    with open('../index/tmp.txt', 'w') as f:
        f.write(str(index.terms))

    pass
