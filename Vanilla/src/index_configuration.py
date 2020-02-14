class IndexConfiguration:

    def __init__(self, stop_words_removal: bool, stemming: bool, normalization: bool):
        self.stop_words_removal = stop_words_removal
        self.stemming = stemming
        self.normalization = normalization
