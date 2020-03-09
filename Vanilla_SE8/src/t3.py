import sys

from PyQt5.Qt import Qt, QApplication

from UI.QueryCompleter import QueryCompleter
from UI.QueryLineEdit import QueryLineEdit


# ref: https://nachtimwald.com/2009/07/04/qcompleter-and-comma-separated-tags/


def main():
    bigrams = ['building technique', 'home address', 'home building', 'address line']
    terms = set()
    for bigram in bigrams:
        tmp = bigram.split()
        terms.add(tmp[0])
        terms.add(tmp[1])
    terms = list(terms)

    app = QApplication(sys.argv)

    editor = QueryLineEdit()

    completer = QueryCompleter(editor, bigrams=bigrams, all_terms=terms)
    completer.setCaseSensitivity(Qt.CaseInsensitive)
    editor.to_complete_following_word.connect(completer.complete_following_term)
    editor.to_complete_term.connect(completer.complete_incomplete_term)
    completer.activated.connect(editor.update_query)
    completer.setWidget(editor)
    editor.show()

    return app.exec_()


if __name__ == '__main__':
    main()
