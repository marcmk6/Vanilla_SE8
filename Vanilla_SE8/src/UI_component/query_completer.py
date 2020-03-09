from PyQt5.Qt import QCompleter, QStringListModel


# ref: https://nachtimwald.com/2009/07/04/qcompleter-and-comma-separated-tags/

class QueryCompleter(QCompleter):

    def __init__(self, parent, all_terms, bigram_model):
        QCompleter.__init__(self, all_terms, parent)
        self.all_terms = all_terms
        self.bigram_model = bigram_model

    def complete_following_term(self, query):
        last_term_in_query = query.split()[-1]
        if last_term_in_query in self.bigram_model.keys():
            candidate_lst = []
            possible_following_terms = self.bigram_model[last_term_in_query]
            for t in possible_following_terms:
                candidate_lst.append(query + t)

            model = QStringListModel(candidate_lst, self)
            self.setModel(model)
            self.setCompletionPrefix(query)
            self.complete()

    def complete_incomplete_term(self, query):
        incomplete_term = query.split()[-1]

        query_before_incomplete_term = '' if query.rfind(' ') == -1 else query[:query.rfind(' ') + 1]
        candidate_lst = []
        for term in self.all_terms:
            if incomplete_term in term:
                candidate_lst.append(query_before_incomplete_term + term)

        if candidate_lst:
            model = QStringListModel(candidate_lst, self)
            self.setModel(model)
            self.setCompletionPrefix(query)
            self.complete()
