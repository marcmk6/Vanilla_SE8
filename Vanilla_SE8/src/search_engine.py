from os import listdir, makedirs, cpu_count
from os.path import isfile, join, exists
from multiprocessing import Pool
from time import time
import logging

from index_configuration import IndexConfiguration
from index import Index, _SearchResult
import vsm_retrieval
import boolean_retrieval
from corpus import Corpus, COURSE_CORPUS_OUTPUT, REUTERS_CORPUS_OUTPUT

INDEX_DIR = '../index/'
INDEX_FILE_EXTENSION = '.idx'
CORPUS_DIR = '../corpus/'
AVAILABLE_CORPUS = {'course_corpus': CORPUS_DIR + 'course_corpus_full.csv',
                    'Reuters': CORPUS_DIR + 'reuters_corpus.csv'}
CORPUS_ID = {'course_corpus': 0, 'Reuters': 1}


class SearchEngine:

    def __init__(self, model='vsm', index_conf=None):
        self.indexes = []
        self.current_se_conf = _SearchEngineConf(model=model, index_conf=index_conf)
        self.corpus_lst = [Corpus(corpus_file=COURSE_CORPUS_OUTPUT), Corpus(corpus_file=REUTERS_CORPUS_OUTPUT)]

    def build_index(self) -> None:
        """
        Build, save and load index
        """
        for corpus, corpus_path in AVAILABLE_CORPUS.items():
            __build_index__(corpus_path=corpus_path, corpus_id=CORPUS_ID[corpus])
        self.load_index()

    def load_index(self) -> None:
        """
        Load existing indexes
        """
        assert self.check_index_integrity()
        self.indexes = []
        index_files = [f for f in listdir(INDEX_DIR) if isfile(join(INDEX_DIR, f)) and f.endswith(INDEX_FILE_EXTENSION)]
        index_files = sorted(index_files, reverse=True)
        for index_file in index_files:
            self.indexes.append(Index.load(INDEX_DIR + index_file))

    def _get_current_index(self):
        current_idx_conf = self.current_se_conf.current_index_conf
        i0 = (lambda x: '0' if x == 'course_corpus' else '1')(self.current_se_conf.current_corpus)
        i1 = int(current_idx_conf.stop_words_removal)
        i2 = int(current_idx_conf.stemming)
        i3 = int(current_idx_conf.normalization)
        i = i0 + str(i1) + str(i2) + str(i3)
        tmp = int(i, base=2)
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

    def query(self, query: str) -> _SearchResult:
        """
        :param query:
        :return: (list of document id, spelling correction object indicating which words are corrected if applicable)
        """
        if self.current_se_conf.current_model == 'vsm':
            return vsm_retrieval.query(self._get_current_index(), query)
        else:
            return boolean_retrieval.query(self._get_current_index(), query)

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
        return _SearchEngineConf.__str__(self.current_se_conf)

    @staticmethod
    def check_index_integrity() -> bool:
        if exists(INDEX_DIR):
            index_files = [f for f in listdir(INDEX_DIR) if
                           isfile(join(INDEX_DIR, f)) and f.endswith(INDEX_FILE_EXTENSION)]
            return len(index_files) == 16
        else:
            return False


def __index_building_worker__(corpus_path, conf_obj, _conf_tuple, corpus_id):
    index = Index(corpus_path=corpus_path, config=conf_obj)
    index_id = ''.join([str(e) for e in list(_conf_tuple)])
    index.save(INDEX_DIR + 'index_' + str(corpus_id) + index_id + INDEX_FILE_EXTENSION)


def __build_index__(corpus_path, corpus_id):
    if not exists(INDEX_DIR):
        makedirs(INDEX_DIR)

    index_confs = []
    _idx_conf_tuples = []
    _range = [1, 0]
    for swr in _range:
        for s in _range:
            for n in _range:
                _idx_conf_tuples.append((swr, s, n))
                index_confs.append(
                    IndexConfiguration(stop_words_removal=bool(swr), stemming=bool(s), normalization=bool(n)))

    pool = Pool(cpu_count())
    params_tuples = []
    for i, j, k, l in zip([corpus_path] * len(index_confs), index_confs, _idx_conf_tuples,
                          [corpus_id] * len(index_confs)):
        params_tuples.append((i, j, k, l))
    start = time()
    with pool:
        pool.starmap(__index_building_worker__, params_tuples)
    print('corpus_id: %s, time spent: %s' % (corpus_id, time() - start))


class _SearchEngineConf:

    def __init__(self, model: str, index_conf=None, corpus='Reuters'):
        if index_conf is not None:
            self.current_index_conf = index_conf
        else:
            self.current_index_conf = IndexConfiguration(stop_words_removal=True, stemming=True, normalization=True)

        self.current_model = model
        self.current_corpus = corpus

    def switch_corpus(self, corpus=None) -> None:
        assert (corpus in AVAILABLE_CORPUS.keys())
        self.current_corpus = corpus

    def switch_model(self, model=None) -> None:
        if model is None:
            current = self.current_model
            if current == 'vsm':
                self.current_model = 'boolean'
            else:
                self.current_model = 'vsm'
        else:
            assert (model in ['vsm', 'boolean'])
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
        return 'Current model: %s, current index selected: %s' % (self.current_model, self.current_index_conf)
