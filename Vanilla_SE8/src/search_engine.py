from os import listdir, makedirs, cpu_count
from os.path import isfile, join, exists
from multiprocessing import Pool
from time import time

from index_configuration import IndexConfiguration
from index import Index, _SearchResult
import vsm_retrieval
import boolean_retrieval
from corpus import Corpus

INDEX_DIR = '../index/'
INDEX_FILE_EXTENSION = '.idx'


class _SearchEngineConf:

    def __init__(self, model: str, index_conf=None):
        if index_conf is not None:
            self.current_index_conf = index_conf
        else:
            self.current_index_conf = IndexConfiguration(stop_words_removal=True, stemming=True, normalization=True)
        self.current_model = model

    def __str__(self):
        return 'Current model: %s, current index selected: %s' % (self.current_model, self.current_index_conf)


class SearchEngine:

    def __init__(self, corpus: str, model='vsm', index_conf=None):
        self.index_confs = []
        self.indexes = []
        self.current_se_conf = _SearchEngineConf(model=model, index_conf=index_conf)
        self.corpus = Corpus(corpus_file=corpus)

    def build_index(self, corpus_path: str) -> None:
        """
        Build and save index from corpus
        """
        __build_index__(corpus_path)
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
        self.index_confs = [idx.config for idx in self.indexes]

    def _get_current_index(self):
        idx_conf = self.current_se_conf.current_index_conf
        i1 = int(idx_conf.stop_words_removal)
        i2 = int(idx_conf.stemming)
        i3 = int(idx_conf.normalization)
        i = str(i1) + str(i2) + str(i3)
        tmp = int(i, base=2)
        return self.indexes[7 - tmp]

    def switch_stop_words_removal(self) -> None:
        current_state = self.current_se_conf.current_index_conf.stop_words_removal
        self.current_se_conf.current_index_conf.stop_words_removal = not current_state

    def switch_stemming(self) -> None:
        current_state = self.current_se_conf.current_index_conf.stemming
        self.current_se_conf.current_index_conf.stemming = not current_state

    def switch_normalization(self) -> None:
        current_state = self.current_se_conf.current_index_conf.normalization
        self.current_se_conf.current_index_conf.normalization = not current_state

    def switch_model(self, model=None) -> None:
        if model is None:
            current = self.current_se_conf.current_model
            if current == 'vsm':
                self.current_se_conf.current_model = 'boolean'
            else:
                self.current_se_conf.current_model = 'vsm'
        else:
            assert (model in ['vsm', 'boolean'])
            self.current_se_conf.current_model = model

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
        return self.corpus.get_doc_content(doc_id)

    def get_doc_title(self, doc_id: str):
        return self.corpus.get_doc_title(doc_id)

    def get_doc_excerpt(self, doc_id: str):
        return self.corpus.get_doc_excerpt(doc_id)

    def __str__(self):
        return _SearchEngineConf.__str__(self.current_se_conf)

    @staticmethod
    def check_index_integrity() -> bool:
        if exists(INDEX_DIR):
            index_files = [f for f in listdir(INDEX_DIR) if
                           isfile(join(INDEX_DIR, f)) and f.endswith(INDEX_FILE_EXTENSION)]
            return len(index_files) == 8
        else:
            return False


def __index_building_worker__(corpus_path, conf_obj, _conf_tuple):
    index = Index(corpus_path=corpus_path, config=conf_obj)
    index_id = ''.join([str(e) for e in list(_conf_tuple)])
    index.save(INDEX_DIR + 'index_' + index_id + INDEX_FILE_EXTENSION)


def __build_index__(corpus_path):
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
    for i, j, k in zip([corpus_path] * len(index_confs), index_confs, _idx_conf_tuples):
        params_tuples.append((i, j, k))
    start = time()
    with pool:
        pool.starmap(__index_building_worker__, params_tuples)
    print('time spent: %s' % (time() - start))
