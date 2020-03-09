import sys

from PyQt5.Qt import Qt, QApplication

from UI_component.query_completer import QueryCompleter
from UI_component.query_line_edit import QueryLineEdit


# ref: https://nachtimwald.com/2009/07/04/qcompleter-and-comma-separated-tags/


def main():
    bigram_model = {'building': ['technique'], 'home': ['address', 'building'], 'address': ['line']}
    terms = set()
    for k,v in bigram_model.items():
        terms.add(k)
        for e in v:
            terms.add(e)
    terms = list(terms)

    app = QApplication(sys.argv)

    query_line_edit = QueryLineEdit()

    query_completer = QueryCompleter(query_line_edit, bigram_model=bigram_model, all_terms=terms)

    query_completer.setCaseSensitivity(Qt.CaseInsensitive)

    query_line_edit.to_complete_following_word.connect(query_completer.complete_following_term)
    query_line_edit.to_complete_term.connect(query_completer.complete_incomplete_term)

    query_completer.activated.connect(query_line_edit.update_query)

    query_completer.setWidget(query_line_edit)
    query_line_edit.show()

    return app.exec_()


if __name__ == '__main__':
    main()
