import csv
import pickle
import numpy as np
from scipy.sparse import csr_matrix
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfTransformer

import dictionary
import text_processing
from wildcard_handler import get_bigrams, bigram_term_matched
from spelling_correction import SpellingCorrection

UNFOUND_TERM_LIMIT = 3


class _Document:

    def __init__(self, doc_id, title, content):
        self.doc_id = doc_id
        self.title = title
        self.content = content


class Index:

    def __init__(self, config=None, corpus_path=None):

        # TODO remove
        self.new = False

        def new_method(config, corpus_path):
            self.config = config
            tmp = self.construct_inverted_index(corpus_path)
            self.terms = tmp[2]
            self.docid_tf_dict = tmp[0]
            self.doc_ids = tmp[3]
            self.doc_count = len(self.doc_ids)
            self.tf_idf_matrix = tmp[1]
            self.tf_over_corpus_dct = tmp[4]
            self.secondary_index = self._build_secondary_index()

        def old_method(config, corpus_path):
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
            self.tf_idf_matrix = self._get_tf_idf_matrix(tf_matrix=self._get_tf_matrix(),
                                                         df_matrix=self._get_df_matrix(),
                                                         n=self.doc_count)

        if self.new:
            new_method(config, corpus_path)
        else:
            old_method(config, corpus_path)

    def construct_inverted_index(self, corpus_path: str) -> (dict, csr_matrix, list, list, dict):

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

        pipe = Pipeline([('count', CountVectorizer(preprocessor=preprocessor)),
                         ('tfidf', TfidfTransformer(use_idf=True, smooth_idf=True, sublinear_tf=True))]).fit(corpus)

        terms = pipe['count'].get_feature_names()

        # total term frequency over corpus
        tf_over_corpus = [sum(e) for e in pipe['count'].transform(corpus).transpose().toarray().tolist()]
        tf_over_corpus_dct = {}
        for i in range(0, len(terms)):
            tf_over_corpus_dct[terms[i]] = tf_over_corpus[i]

        # tf-idf matrix
        tfidf_matrix = pipe.transform(corpus)
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
    def _get_tf_idf_matrix(tf_matrix: csr_matrix, df_matrix: csr_matrix, n: int) -> csr_matrix:
        """
        Calculate tf-idf
        :param tf_matrix: term frequency, dimension (d,v)
        :param df_matrix: document frequency, dimension (d,v)
        :param n: number of documents
        :return: tf-idf in csr_matrix format, dimension (d,v)
        """
        df_matrix.data = np.log10(n / (df_matrix.data + 1))
        tf_matrix.data = np.log10(1 + tf_matrix.data)
        tmp = df_matrix.multiply(tf_matrix)
        return tmp

    def _get_df_matrix(self) -> csr_matrix:
        """
        Calculate raw document frequency
        :return: csr_matrix (d, v)
        """
        df_lst = list(self.df_dict.values())
        tmp = [df_lst] * self.doc_count
        tmp = csr_matrix(tmp)
        return tmp

    def _get_tf_matrix(self) -> csr_matrix:
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

        tf_matrix = np.asarray(lst).transpose()
        tf_matrix = csr_matrix(tf_matrix)
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
        # FIXME
        if not self.new:
            dct = self.docid_tf_dict[term]
            return sum(list(dct.values()))
        else:
            return self.tf_over_corpus_dct[term]


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
