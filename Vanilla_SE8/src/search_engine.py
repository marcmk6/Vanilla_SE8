from os import listdir, makedirs, cpu_count
from os.path import isfile, join, exists
from time import time
import ray

from intermediate_class.index_configuration import IndexConfiguration
from retrieval_model import boolean_retrieval, vsm_retrieval
from intermediate_class.corpus import Corpus
from global_variable import INDEX_DIR, INDEX_FILE_EXTENSION, ALL_POSSIBLE_INDEX_CONFIGURATIONS, TMP_AVAILABLE_CORPUS, \
    VSM_MODEL, BOOLEAN_MODEL, QUERY_MODELS, COURSE_CORPUS, REUTERS_CORPUS, QUERY_COMPLETION_FILE_EXTENSION
from index_v2 import Index_v2
from intermediate_class.search_result import SearchResult


class SearchEngine:

    def __init__(self, model=VSM_MODEL, index_conf=None):
        self.indexes = []
        self.current_se_conf = _SearchEngineConf(model=model, index_conf=index_conf)
        self.corpus_lst = [Corpus(corpus_file=COURSE_CORPUS), Corpus(corpus_file=REUTERS_CORPUS)]
        self.query_completion_lst = []

    def build_index(self) -> None:
        """
        Build, save and load index
        """
        for corpus, corpus_path in TMP_AVAILABLE_CORPUS.items():
            __build_index__(corpus_path=corpus_path)
        self.load_index()

    def load_index(self) -> None:
        """
        Load existing indexes
        """
        import pickle

        assert self.check_index_integrity()
        self.indexes = []
        index_files = [f for f in listdir(INDEX_DIR) if isfile(join(INDEX_DIR, f)) and f.endswith(INDEX_FILE_EXTENSION)]
        index_files = sorted(index_files, reverse=True)
        for index_file in index_files:
            self.indexes.append(Index_v2.load(INDEX_DIR + index_file))

        query_completion_files = [f for f in listdir(INDEX_DIR)
                                  if isfile(join(INDEX_DIR, f)) and f.endswith(QUERY_COMPLETION_FILE_EXTENSION)]
        query_completion_files = sorted(query_completion_files)
        for qc in query_completion_files:
            with open(INDEX_DIR + qc, 'rb') as f:
                qc_obj = pickle.load(f)
            self.query_completion_lst.append(qc_obj)

        pass

    def get_query_completion_obj(self, i):
        # qc_idx = 0 if self.current_se_conf.current_corpus == 'course_corpus' else 1
        return self.query_completion_lst[i]

    def _get_current_index(self):
        tmp = int(str(self.current_se_conf), base=2)
        return self.indexes[15 - tmp]

    def switch_stop_words_removal(self) -> None:
        """Switch stopwords removal"""
        self.current_se_conf.switch_stop_words_removal()

    def switch_stemming(self) -> None:
        """Switch stemming"""
        self.current_se_conf.switch_stemming()

    def switch_normalization(self) -> None:
        """Switch normalization"""
        self.current_se_conf.switch_normalization()

    def switch_model(self, model=None) -> None:
        """Switch retrieval model"""
        self.current_se_conf.switch_model(model=model)

    def switch_corpus(self, corpus=None) -> None:
        """Switch corpus"""
        self.current_se_conf.switch_corpus(corpus=corpus)

    def switch_query_expansion(self) -> None:
        self.current_se_conf.switch_query_expansion()

    def query(self, query: str) -> SearchResult:
        """
        :param query:
        :return: (list of document id, spelling correction object indicating which words are corrected if applicable)
        """
        if self.current_se_conf.current_model == VSM_MODEL:
            return vsm_retrieval.query(self._get_current_index(), query)
        else:
            return boolean_retrieval.query(self._get_current_index(), query)

    def expand_query_globally(self):
        pass

    def get_doc_content(self, doc_id: str):
        current_corpus = self.corpus_lst[
            (lambda x: 0 if x == 'course_corpus' else 1)(self.current_se_conf.current_corpus)]
        return current_corpus.get_doc_content(doc_id)

    def get_doc_title(self, doc_id: str):
        current_corpus = self.corpus_lst[
            (lambda x: 0 if x == 'course_corpus' else 1)(self.current_se_conf.current_corpus)]
        return current_corpus.get_doc_title(doc_id)

    def get_doc_excerpt(self, doc_id: str):
        current_corpus = self.corpus_lst[
            (lambda x: 0 if x == 'course_corpus' else 1)(self.current_se_conf.current_corpus)]
        return current_corpus.get_doc_excerpt(doc_id)

    def __str__(self):
        return self.current_se_conf

    @staticmethod
    def check_index_integrity() -> bool:
        if exists(INDEX_DIR):
            index_files = [f for f in listdir(INDEX_DIR) if
                           isfile(join(INDEX_DIR, f)) and f.endswith(INDEX_FILE_EXTENSION)]
            return len(index_files) == 16
        else:
            return False


def __build_index__(corpus_path):
    if not exists(INDEX_DIR):
        makedirs(INDEX_DIR)

    start = time()
    ray.init(num_cpus=cpu_count())
    for ic in ALL_POSSIBLE_INDEX_CONFIGURATIONS:
        Index_v2(corpus=corpus_path, index_conf=ic).build()
    ray.shutdown()
    print('Total time building index %s: %s' % (time() - start, '0' if 'course' in corpus_path else '1'))


class _SearchEngineConf:

    def __init__(self, model: str, index_conf=None, corpus='course_corpus'):
        if index_conf is not None:
            self.current_index_conf = index_conf
        else:
            self.current_index_conf = IndexConfiguration(stop_words_removal=True, stemming=True, normalization=True)

        self.current_model = model
        self.current_corpus = corpus

    def switch_corpus(self, corpus=None) -> None:
        assert (corpus in TMP_AVAILABLE_CORPUS.keys())
        self.current_corpus = corpus

    def switch_model(self, model=None) -> None:
        if model is None:
            current = self.current_model
            if current == VSM_MODEL:
                self.current_model = BOOLEAN_MODEL
            else:
                self.current_model = VSM_MODEL
        else:
            assert (model in QUERY_MODELS)
            self.current_model = model

    def switch_stop_words_removal(self) -> None:
        current_state = self.current_index_conf.stop_words_removal
        self.current_index_conf.stop_words_removal = not current_state

    def switch_stemming(self) -> None:
        current_state = self.current_index_conf.stemming
        self.current_index_conf.stemming = not current_state

    def switch_normalization(self) -> None:
        current_state = self.current_index_conf.normalization
        self.current_index_conf.normalization = not current_state

    def switch_query_expansion(self) -> None:
        # TODO implement
        pass

    def __str__(self):
        corpus = '0' if self.current_corpus == 'course_corpus' else '1'
        return corpus + str(self.current_index_conf)


if __name__ == '__main__':
    se = SearchEngine(model='vsm')
    se.build_index()
