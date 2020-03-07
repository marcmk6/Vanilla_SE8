class IndexConfiguration:

    def __init__(self, stop_words_removal: bool, stemming: bool, normalization: bool):
        self.stop_words_removal = stop_words_removal
        self.stemming = stemming
        self.normalization = normalization

    def __str__(self):
        return str(int(self.stop_words_removal)) + str(int(self.stemming)) + str(int(self.normalization))


def __get_all_possible_index_configurations__():
    lst = []
    _range = [0, 1]
    for swr in _range:
        for s in _range:
            for n in _range:
                lst.append(
                    IndexConfiguration(stop_words_removal=bool(swr), stemming=bool(s), normalization=bool(n)))
    return lst
