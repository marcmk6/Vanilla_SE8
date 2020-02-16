class IndexConfiguration:

    def __init__(self, stop_words_removal: bool, stemming: bool, normalization: bool):
        self.stop_words_removal = stop_words_removal
        self.stemming = stemming
        self.normalization = normalization

    def __str__(self):
        return 'stop_words_removal: %s, stemming: %s, normalization: %s' % (
        self.stop_words_removal, self.stemming, self.normalization)
