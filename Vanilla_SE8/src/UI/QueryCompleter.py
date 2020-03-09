from PyQt5.Qt import QCompleter, QStringListModel


# ref: https://nachtimwald.com/2009/07/04/qcompleter-and-comma-separated-tags/

class QueryCompleter(QCompleter):

    def __init__(self, parent, all_terms, bigrams):
        QCompleter.__init__(self, all_terms, parent)
        self.all_terms = all_terms
        self.bigrams = bigrams

    def complete_following_term(self, query):
        lst = []
        query_tokens = query.split()
        for bigram in self.bigrams:
            _bigram = bigram.split()
            if query_tokens[-1] == _bigram[0]:
                lst.append(query + _bigram[1])
        model = QStringListModel(lst, self)
        self.setModel(model)

        self.setCompletionPrefix(query)
        # if query.strip() != '':
        self.complete()

    def complete_incomplete_term(self, query):
        incomplete_term = query.split()[-1]

        query_before_incomplete_term = '' if query.rfind(' ') == -1 else query[:query.rfind(' ') + 1]
        lst = []
        for term in self.all_terms:
            if incomplete_term in term:
                lst.append(query_before_incomplete_term + term)
        model = QStringListModel(lst, self)
        self.setModel(model)
        self.setCompletionPrefix(query)
        # if incomplete_term.strip() != '':
        self.complete()
