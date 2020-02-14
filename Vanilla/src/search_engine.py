from os import listdir, makedirs
from os.path import isfile, join, exists
from index_configuration import IndexConfiguration
from index import Index
import vsm_retrieval
import boolean_retrieval

INDEX_DIR = '../index/'


class _SEConf:

    def __init__(self, model: str, index_conf=None):
        if index_conf is not None:
            self.current_index_conf = index_conf
        else:
            self.current_index_conf = IndexConfiguration(stop_words_removal=True, stemming=True, normalization=True)
        self.current_model = model


class SearchEngine:

    def __init__(self, model='vsm', index_conf=None):
        self.index_confs = []
        self.indexes = []
        self.current_se_conf = _SEConf(model=model, index_conf=index_conf)

    def build_index(self, corpus_path: str):

        if not exists(INDEX_DIR):
            makedirs(INDEX_DIR)

        self.index_confs = []
        _conf_tuples = []
        _range = [1, 0]
        for swr in _range:
            for s in _range:
                for n in _range:
                    _conf_tuples.append((swr, s, n))
                    self.index_confs.append(
                        IndexConfiguration(stop_words_removal=bool(swr), stemming=bool(s), normalization=bool(n)))

        self.indexes = []
        for _conf_tuple, conf_obj in zip(_conf_tuples, self.index_confs):
            index = Index(corpus=corpus_path, config=conf_obj)
            index.save(INDEX_DIR + 'index_' + ''.join([str(e) for e in list(_conf_tuple)]) + '.idx')
            self.indexes.append(index)

    def load_index(self):
        self.indexes = []
        index_files = [f for f in listdir(INDEX_DIR) if isfile(join(INDEX_DIR, f))]
        index_files = sorted(index_files, reverse=True)
        for index_file in index_files:
            self.indexes.append(Index.load(index_file))
        self.index_confs = [idx.config for idx in self.indexes]

    def _get_current_index(self):
        idx_conf = self.current_se_conf.current_index_conf
        i1 = int(idx_conf.stop_words_removal)
        i2 = int(idx_conf.stemming)
        i3 = int(idx_conf.normalization)
        i = str(i1) + str(i2) + str(i3)
        return self.indexes[int(i, base=2)]

    def switch_stop_words_removal(self) -> None:
        self.current_se_conf.stop_words_removal = not self.current_se_conf.stop_words_removal

    def switch_stemming(self) -> None:
        self.current_se_conf.stemming = not self.current_se_conf.stemming

    def switch_normalization(self) -> None:
        self.current_se_conf.normalization = not self.current_se_conf.normalization

    def switch_model(self) -> None:
        current = self.current_se_conf.current_model
        if current == 'vsm':
            self.current_se_conf.current_model = 'boolean'
        else:
            self.current_se_conf.current_model = 'vsm'

    def query(self, query: str) -> list:
        if self.current_se_conf.current_model == 'vsm':
            return vsm_retrieval.query(self.indexes[self._get_current_index()], query)
        else:
            return boolean_retrieval.query(self.indexes[self._get_current_index()], query)


if __name__ == '__main__':
    # conf = Configuration(stop_words_removal=True, stemming=True, normalization=True)
    corpus_path = '../course_corpus_full.csv'
    # idx = Index(corpus=corpus_path, config=conf)
    # idx.save('idx_full_b')
    # idx = Index.load('idx_full_b')
    # print(idx.doc_count)

    se = SearchEngine(model='vsm')
    se.build_index(corpus_path)
