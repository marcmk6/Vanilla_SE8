import csv
import dictionary
import json
import numpy as np
import text_processing

SAVING_KEY_DF_DICT = 'DF_DICT'
SAVING_KEY_DOCID_TF_DICT = 'DOCID_TF_DICT'
SAVING_KEY_DOC_ID = 'DOC_ID'


class Index:
    class Document:

        def __init__(self, doc_id, title, content):
            self.doc_id = doc_id
            self.title = title
            self.content = content

    def __init__(self, corpus=None, df_dict=None, docid_tf_dict=None, doc_ids=None):
        """
        The index can be either construct from a corpus or from dicts
        :param corpus: path to corpus file
        :param df_dict:
        :param docid_tf_dict:
        :param doc_ids:
        """
        if corpus:
            terms = dictionary.build_vocabulary(corpus)
            docs = self._create_documents(corpus)
            id_lst = []
            for doc in docs:
                id_lst.append(doc.doc_id)
            tf, df = self._construct_index(terms, docs)

            self.df_dict = df
            self.docid_tf_dict = tf
            self.doc_ids = {SAVING_KEY_DOC_ID: id_lst}
        elif (df_dict is not None) and (docid_tf_dict is not None) and (doc_ids is not None):
            self.df_dict = df_dict
            self.docid_tf_dict = docid_tf_dict
            self.doc_ids = doc_ids
        else:
            pass

        self.doc_count = len(self.doc_ids[SAVING_KEY_DOC_ID])

        tf_matrix = self._get_tf_matrix()
        df_matrix = self._get_df_matrix()
        self.tf_idf_matrix = self._get_tf_idf_matrix(tf_matrix, df_matrix, n=self.doc_count)

    @staticmethod
    def _create_documents(corpus):
        """
        Create Document objects from corpus
        :param corpus: path to the corpus file
        :return: list of Document objects
        """
        docs = []
        with open(corpus, 'r') as corpus_file:
            reader = csv.reader(corpus_file)
            for row in reader:
                doc_id = row[0]
                title = row[1]
                content = row[2]
                docs.append(Index.Document(doc_id, title, content))
        return docs

    @staticmethod
    def _construct_index(terms, documents) -> (dict, dict):
        df_dict = {}  # k: term, v: df
        docid_tf_dict = {}  # k: term, v: postings (dict:{doc_id, tf})

        for term in terms:
            df_dict[term] = 0
            docid_tf_dict[term] = {}

        for doc in documents:
            doc_id = doc.doc_id
            search_field = ' '.join(
                text_processing.process(doc.title + doc.content, stop_words_removal=True, stemming=True,
                                        normalization=True))
            for term in terms:
                count = search_field.count(term)
                if count > 0:
                    df_dict[term] += 1
                    docid_tf_dict[term][doc_id] = count

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
        return np.log10(n / df_arr) * np.log10(1 + tf_arr)

    def _get_df_matrix(self) -> np.ndarray:
        """
        :return: numpy.ndarray (d, v)
        """
        vocabulary_size = len(self.df_dict.keys())

        df_lst = list(self.df_dict.values())

        tmp = df_lst * self.doc_count
        tmp = np.asarray(tmp).reshape((self.doc_count, vocabulary_size))

        return tmp

    def _get_tf_matrix(self) -> np.ndarray:
        """
        :return:  numpy.ndarray (d,v)
        """

        def _get_idx(doc_id: str) -> int:
            return self.doc_ids[SAVING_KEY_DOC_ID].index(doc_id)

        vocabulary_size = len(self.df_dict.keys())

        lst = []
        terms = self.df_dict.keys()
        for term in terms:
            col = [0] * self.doc_count
            tmp = self.docid_tf_dict[term]
            for doc_id, tf in tmp.items():
                idx = _get_idx(doc_id)
                col[idx] = tf
            lst += col

        return np.asarray(lst).reshape((self.doc_count, vocabulary_size))

    def save(self, out):
        with open(out, 'w') as f:
            f.write(json.dumps({SAVING_KEY_DF_DICT: self.df_dict, SAVING_KEY_DOCID_TF_DICT: self.docid_tf_dict,
                                SAVING_KEY_DOC_ID: self.doc_ids}))

    @staticmethod
    def load(index_file):
        with open(index_file, 'r') as f:
            idx = json.load(f)
        return Index(df_dict=idx[SAVING_KEY_DF_DICT], docid_tf_dict=idx[SAVING_KEY_DOCID_TF_DICT],
                     doc_ids=idx[SAVING_KEY_DOC_ID])

    def get(self, keyword: str) -> list:
        if keyword in self.docid_tf_dict.keys():
            return list(self.docid_tf_dict[keyword].keys())
        else:
            return []


if __name__ == '__main__':
    corpus_path = '../course_corpus.csv'

    # print(tf)
    # print({k: v for k, v in sorted(df.items(), key=lambda item: item[1], reverse=True)})

    # idxf2 = Index(corpus = corpus_path)

    idx = Index.load('INDEX')
    print(idx.get('plane'))
