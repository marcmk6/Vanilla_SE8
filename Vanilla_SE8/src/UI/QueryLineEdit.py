from PyQt5.QtCore import pyqtSignal
from PyQt5.Qt import QLineEdit


# ref: https://nachtimwald.com/2009/07/04/qcompleter-and-comma-separated-tags/


class QueryLineEdit(QLineEdit):
    to_complete_following_word = pyqtSignal(str)
    to_complete_term = pyqtSignal(str)

    def __init__(self, *args):
        QLineEdit.__init__(self, *args)
        self.textChanged.connect(self.text_changed)

    def text_changed(self, query):
        if len(query) > 0:
            if query[-1] == ' ':
                # complete next term
                self.to_complete_following_word.emit(query)
            else:
                # complete incomplete term
                self.to_complete_term.emit(query)

    def update_query(self, text):
        # print('txt:%s' % text)
        # cursor_pos = self.cursorPosition()
        # print('cursor_pos:%s' % cursor_pos)
        # before_text = (self.text())[:cursor_pos]
        # print('before_text:%s' % before_text)
        # after_text = (self.text())[cursor_pos:]
        # print('after_text:%s' % before_text)
        # prefix_len = len(before_text.split(',')[-1].strip())
        #
        # self.setText('%s%s, %s' % (before_text[:cursor_pos - prefix_len], text,
        #                            after_text))
        # self.setCursorPosition(cursor_pos - prefix_len + len(text) + 2)
        #
        self.setText(text)
