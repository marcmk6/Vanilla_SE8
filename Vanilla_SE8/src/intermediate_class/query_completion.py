import pickle


class QueryCompletion:

    def __init__(self, bigram_model, all_terms):
        self.bigram_model = bigram_model
        self.all_terms = all_terms

    @staticmethod
    def load(qc_file):
        with open(qc_file, 'rb') as f:
            qc = pickle.load(f)
        return qc
