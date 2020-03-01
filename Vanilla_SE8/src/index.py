import csv
import dictionary
import pickle
import numpy as np
import re
import shelve

import text_processing
from wildcard_handler import get_bigrams, bigram_2_regex
from spelling_correction import SpellingCorrection

UNFOUND_TERM_LIMIT = 3


class _Document:

    def __init__(self, doc_id, title, content):
        self.doc_id = doc_id
        self.title = title
        self.content = content


class Index:

    def __init__(self, config=None, corpus_path=None):
        """
        The index can be either construct from a corpus or from dicts
        :param config:
        :param corpus_path:
        """

        self.config = config
        self.terms = dictionary.build_vocabulary(corpus_path, self.config)

        docs = self._create_documents(corpus_path)
        id_lst = []
        for doc in docs:
            id_lst.append(doc.doc_id)
        tf, df = self._construct_index(self.terms, docs)

        self.df_dict = df
        self.docid_tf_dict = tf
        self.doc_ids = id_lst

        self.doc_count = len(self.doc_ids)

        tf_matrix = self._get_tf_matrix()
        df_matrix = self._get_df_matrix()
        self.tf_idf_matrix = self._get_tf_idf_matrix(tf_matrix, df_matrix, n=self.doc_count)

        self.secondary_index = self._build_secondary_index()

    @staticmethod
    def _create_documents(corpus_path):
        """
        Create Document objects from corpus
        :param corpus_path: path to the corpus file
        :return: list of Document objects
        """
        docs = []
        with open(corpus_path, 'r') as corpus_file:
            reader = csv.reader(corpus_file)
            for row in reader:
                doc_id = row[0]
                title = row[1]
                content = row[2]
                docs.append(_Document(doc_id, title, content))
        return docs

    def _construct_index(self, terms, documents) -> (dict, dict):
        df_dict = {}  # k: term, v: df
        docid_tf_dict = {}  # k: term, v: postings (dict:{doc_id, tf})

        for term in terms:
            df_dict[term] = 0
            docid_tf_dict[term] = {}

        for doc in documents:
            doc_id = doc.doc_id
            search_field = ' '.join(
                text_processing.process(string=doc.title + doc.content, config=self.config))
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
    def _get_tf_idf_matrix(tf_arr: np.ndarray, df_arr: np.ndarray, n: int) -> np.ndarray:
        """
        Calculate tf-idf
        :param tf_arr: term frequency, dimension (d,v)
        :param df_arr: document frequency, dimension (d,v)
        :param n: number of documents
        :return: tf-idf in numpy.ndarray format, dimension (d,v)
        """
        return np.log10(n / (df_arr + 1)) * np.log10(1 + tf_arr)

    def _get_df_matrix(self) -> np.ndarray:
        """
        Calculate raw document frequency
        :return: numpy.ndarray (d, v)
        """
        vocabulary_size = len(self.df_dict.keys())

        df_lst = list(self.df_dict.values())

        tmp = [df_lst] * self.doc_count
        tmp = np.asarray(tmp).reshape((self.doc_count, vocabulary_size))

        return tmp

    def _get_tf_matrix(self) -> np.ndarray:
        """
        Calculate raw term frequency
        :return:  numpy.ndarray (d,v)
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

        tf_matrix = np.asarray(lst)
        tf_matrix = np.transpose(tf_matrix)
        return tf_matrix

    def get(self, term: str) -> list:
        if term in self.docid_tf_dict.keys():
            return list(self.docid_tf_dict[term].keys())
        else:
            return []

    def save(self, out) -> None:
        self._save_pickle(out)

    def _save_pickle(self, out) -> None:
        """
        Save the Index object by serializing
        :param out: path to the output
        :return: None
        """
        with open(out, 'wb') as f:
            pickle.dump(self, f)

    def _save_shelve(self, out) -> None:
        s = shelve.open(out)
        s['config'] = self.config
        s['df_dict'] = self.df_dict
        s['doc_count'] = self.doc_count
        s['docid_tf_dict'] = self.docid_tf_dict
        s['secondary_index'] = self.secondary_index
        s['terms'] = self.terms
        s['tf_idf_matrix'] = self.tf_idf_matrix
        s.close()

    @staticmethod
    def load(index_file):
        return Index._load_pickle(index_file)

    @staticmethod
    def _load_pickle(index_file):
        """
        Load the Index object
        :param index_file: path to the index file
        :return: index
        """
        with open(index_file, 'rb') as f:
            index = pickle.load(f)
        return index

    def __str__(self):

        return ''

    def _build_secondary_index(self):

        secondary_index = {}

        all_bigrams = set()
        for term in self.terms:
            if re.search('^[a-zA-Z]{2,}$', term) is not None:
                all_bigrams = all_bigrams.union(get_bigrams(term))

        for bigram in list(all_bigrams):
            matched_terms = []
            for term in self.terms:
                if re.search(bigram_2_regex(bigram), term) is not None:
                    matched_terms.append(term)
            secondary_index[bigram] = sorted(matched_terms)  # FIXME: sort or not?

        return secondary_index

    def get_term_frequency(self, term: str) -> int:
        dct = self.docid_tf_dict[term]
        return sum(list(dct.values()))


def bigrams_2_terms(index: Index, bigrams: set) -> set:
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
