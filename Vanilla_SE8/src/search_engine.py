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
from util.global_query_expansion import expand_query_globally
from intermediate_class.query_completion import QueryCompletion
from util.topic_handler import TopicHandler
from util.relevance_feedback import RelevanceFeedbackSession, RelevanceFeedback


class SearchEngine:

    def __init__(self, model=VSM_MODEL, index_conf=None):
        self.indexes = []
        self.current_se_conf = _SearchEngineConf(model=model, index_conf=index_conf)
        self.corpus_lst = [Corpus(corpus_file=COURSE_CORPUS), Corpus(corpus_file=REUTERS_CORPUS)]
        self.query_completion_lst = []
        self.all_topics = []  # reuters
        self.currently_selected_topics = []
        self.__topic_handler__ = TopicHandler()
        self.rf_session = RelevanceFeedbackSession()

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

        # load inverted index and tf-idf index
        assert self.check_index_integrity()
        self.indexes = []
        index_files = [f for f in listdir(INDEX_DIR) if isfile(join(INDEX_DIR, f)) and f.endswith(INDEX_FILE_EXTENSION)]
        index_files = sorted(index_files, reverse=True)
        for index_file in index_files:
            self.indexes.append(Index_v2.load(INDEX_DIR + index_file))

        # load query completion data
        query_completion_files = [f for f in listdir(INDEX_DIR)
                                  if isfile(join(INDEX_DIR, f)) and f.endswith(QUERY_COMPLETION_FILE_EXTENSION)]
        query_completion_files = sorted(query_completion_files)
        for qc_file_name in query_completion_files:
            qc_obj = QueryCompletion.load(INDEX_DIR + qc_file_name)
            self.query_completion_lst.append(qc_obj)

        # load reuters topics
        self.all_topics = self.__topic_handler__.get_all_topics()
        self.currently_selected_topics = self.all_topics
        pass

    def add_relevance_feedback(self, query: str, p_doc_ids: list, n_doc_ids: list):
        def vectorize_doc(doc_id):
            content = self.get_doc_content(doc_id)
            vec = vsm_retrieval.vectorize_query(self._get_current_index(), content)[0]
            return vec

        p_doc_vecs = [vectorize_doc(doc_id) for doc_id in p_doc_ids]
        n_doc_vecs = [vectorize_doc(doc_id) for doc_id in n_doc_ids]
        query_vec = vsm_retrieval.vectorize_query(self._get_current_index(), query)[0]
        rf = RelevanceFeedback(query_vec, p_doc_vecs, n_doc_vecs)
        self.rf_session.add_relevance_feedback(query, rf)

    def get_all_topics(self):
        return self.all_topics

    def switch_all_selection(self):
        if len(self.currently_selected_topics) < len(self.all_topics):
            self.set_selected_topics(self.all_topics)
        else:
            self.set_selected_topics([])

    def set_selected_topics(self, topic_list):
        self.currently_selected_topics = topic_list

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

    def query(self, query: str) -> SearchResult:
        """
        :param query:
        :return: (list of document id, spelling correction object indicating which words are corrected if applicable)
        """

        if self.current_se_conf.current_model == VSM_MODEL:
            query_result = vsm_retrieval.query(self._get_current_index(), query, self.rf_session)
        else:
            query_result = boolean_retrieval.query(self._get_current_index(), query)

        # filter results by topic, Reuters only
        if self.current_se_conf.current_corpus == 'Reuters':
            if len(self.currently_selected_topics) < len(self.all_topics):
                selected_doc_id_range = self.__topic_handler__.get_docids_with_topics(
                    self.currently_selected_topics)
                query_result.filter_by_doc_ids(selected_doc_id_range)

        return query_result

    @staticmethod
    def expand_query_globally(query: str) -> str:
        return expand_query_globally(query)

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

    def __str__(self):
        corpus = '0' if self.current_corpus == 'course_corpus' else '1'
        return corpus + str(self.current_index_conf)


if __name__ == '__main__':
    se = SearchEngine(model='vsm')
    se.load_index()
    se.switch_corpus('Reuters')
    pass
    r = se.vectorize_doc('21004')[0]
    count = 0
    for e in r.tolist():
        if e != 0:
            count += 1
    pass
    # se.build_index()
