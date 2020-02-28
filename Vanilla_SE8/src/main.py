import os
import sys

from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QWidget, QApplication, QLineEdit, QPushButton, QRadioButton, QMessageBox, QHBoxLayout, \
    QVBoxLayout, QLabel, QButtonGroup, QTextEdit, QListView, QDialog, QCheckBox
from PyQt5.QtGui import QIcon, QStandardItemModel, QStandardItem
from PyQt5.QtCore import pyqtSlot

from search_engine import SearchEngine
from corpus import preprocess_course_corpus

BOOLEAN_MODEL_BUTTON_TEXT = 'Boolean Model'
VSM_MODEL_BUTTON_TEXT = 'VSM Model'
tmp_corpus = '../corpus/reuters_corpus.csv'

# Inspired from https://pythonspot.com/gui/
# Modele 1 - User Interface
class MainWindow(QWidget):
    """
    This is the graphical user interface
    """

    def __init__(self):
        super().__init__()
        self.setup_se()
        self.initUI()

    def setup_se(self):
        if not os.path.exists(tmp_corpus):
            preprocess_course_corpus()
        self.search_engine = SearchEngine(corpus=tmp_corpus, model='vsm')  # FIXME
        if not SearchEngine.check_index_integrity():
            self.__create_message_box('Please wait for the construction of index.\n'
                                      'This may take about 1 minute.\n'
                                      'Click OK to start.')
            self.search_engine.build_index(corpus_path=tmp_corpus)
        else:
            self.search_engine.load_index()

    def initUI(self):
        # setup entire layout
        vbox = QVBoxLayout()
        self.setLayout(vbox)

        # default size
        self.resize(640, 480)

        # main window title
        self.setWindowTitle('Vanilla_SE8 SE 8')

        # main window icon
        self.setWindowIcon(QIcon(os.path.dirname(
            os.path.abspath(__file__)) + '/icons/vanilla.png'))

        # add query input field
        searchLayout = QHBoxLayout()
        searchLabel = QLabel('Query: ')
        searchLayout.addWidget(searchLabel)
        self.searchField = QLineEdit()
        # self.searchField.move(20, 20)
        # self.searchField.resize(200, 40)
        searchLayout.addWidget(self.searchField)
        vbox.addLayout(searchLayout)

        # add choice of model, Boolean, or VSM
        modelLabel = QLabel('Choice of model: \t\t')
        modelLayout = QHBoxLayout()
        modelGroup = QButtonGroup(self)
        self.button_boolean_model = QRadioButton(BOOLEAN_MODEL_BUTTON_TEXT)
        self.button_boolean_model.setChecked(False)
        self.button_vsm_model = QRadioButton(VSM_MODEL_BUTTON_TEXT)
        self.button_vsm_model.setChecked(True)
        self.button_boolean_model.toggled.connect(lambda: self.changeChoiceState(self.button_boolean_model))
        self.button_vsm_model.toggled.connect(lambda: self.changeChoiceState(self.button_vsm_model))
        modelLayout.addWidget(modelLabel)
        modelLayout.addWidget(self.button_boolean_model)
        modelGroup.addButton(self.button_boolean_model)
        modelLayout.addWidget(self.button_vsm_model)
        modelGroup.addButton(self.button_vsm_model)
        modelLayout.addStretch(1)
        vbox.addLayout(modelLayout)

        # add choice of collection: UofO catalog, Reuters
        collectionLabel = QLabel('Choice of collection: \t')
        collectionLayout = QHBoxLayout()
        collectionButtonGroup = QButtonGroup(self)
        self.button_uo_courses = QRadioButton('UofO catalog')
        self.button_uo_courses.setChecked(True)
        self.button_uo_courses.toggled.connect(lambda: self.changeChoiceState(self.button_uo_courses))
        collectionLayout.addWidget(collectionLabel)
        collectionLayout.addWidget(self.button_uo_courses)
        collectionButtonGroup.addButton(self.button_uo_courses)
        collectionLayout.addStretch(1)
        # Reuters
        self.button_reuters = QRadioButton('Reuters')
        self.button_reuters.setChecked(False)
        self.button_reuters.toggled.connect(lambda: self.changeChoiceState(self.button_reuters))
        collectionLayout.addWidget(collectionLabel)
        collectionLayout.addWidget(self.button_reuters)
        collectionButtonGroup.addButton(self.button_reuters)
        collectionLayout.addStretch(1)
        vbox.addLayout(collectionLayout)

        # Index configuration selection
        # sw_rm_label = QLabel('Remove stopwords: \t')
        sw_rm_label = QLabel('')
        sw_rm_layout = QHBoxLayout()
        self.sw_rm_btn = QCheckBox('Remove stopwords')
        self.sw_rm_btn.setChecked(True)
        self.sw_rm_btn.stateChanged.connect(lambda: self.btnstate(self.sw_rm_btn))
        self.sw_rm_btn.setToolTip('Perform stopwords removal on both query and corpus')
        sw_rm_layout.addWidget(sw_rm_label)
        sw_rm_layout.addWidget(self.sw_rm_btn)
        vbox.addLayout(sw_rm_layout)

        # stm_label = QLabel('Stemming: \t')
        stm_label = QLabel('')
        stm_layout = QHBoxLayout()
        self.stm_btn = QCheckBox('Stemming')
        self.stm_btn.setChecked(True)
        self.stm_btn.stateChanged.connect(lambda: self.btnstate(self.stm_btn))
        self.stm_btn.setToolTip('Perform stemming on both query and corpus')
        stm_layout.addWidget(stm_label)
        stm_layout.addWidget(self.stm_btn)
        vbox.addLayout(stm_layout)

        # nm_label = QLabel('Normalization: \t')
        nm_label = QLabel('')
        nm_layout = QHBoxLayout()
        self.nm_btn = QCheckBox('Normalization')
        self.nm_btn.setChecked(True)
        self.nm_btn.stateChanged.connect(lambda: self.btnstate(self.nm_btn))
        self.nm_btn.setToolTip(
            'Perform normalization on both query and corpus. (e.g. low-cost -> low cost, U.S.A -> USA)')
        nm_layout.addWidget(nm_label)
        nm_layout.addWidget(self.nm_btn)
        vbox.addLayout(nm_layout)

        # TODO: the information needed for the spelling correction

        # add a search button
        searchButtonLayout = QHBoxLayout()
        self.searchButton = QPushButton('Search')
        # self.searchButton.move(20, 400)
        self.searchButton.clicked.connect(self.click_search)
        searchButtonLayout.addStretch(1)
        searchButtonLayout.addWidget(self.searchButton)
        vbox.addLayout(searchButtonLayout)

        # add a qeury result listView
        self.retrieved_doc_ids = []  # record the doc IDs
        queryResultLayout = QHBoxLayout()
        queryResultLabel = QLabel('Query result: \t')
        self.queryResult = QListView()
        self.queryResult.setAcceptDrops(False)
        self.queryResult.setDragDropMode(
            QtWidgets.QAbstractItemView.NoDragDrop)
        self.queryResult.setSelectionMode(
            QtWidgets.QAbstractItemView.ExtendedSelection)
        self.queryResult.setResizeMode(QListView.Fixed)
        self.queryResult.clicked.connect(self.selectItem)
        queryResultLayout.addWidget(queryResultLabel)
        queryResultLayout.addWidget(self.queryResult)
        # queryResultLayout.addStretch(1)
        vbox.addLayout(queryResultLayout)

        # centerize main window
        self.center()

        self.show()

    def btnstate(self, b):
        if b.text() == 'Remove stopwords':
            self.search_engine.switch_stop_words_removal()
        elif b.text() == 'Stemming':
            self.search_engine.switch_stemming()
        elif b.text() == 'Normalization':
            self.search_engine.switch_normalization()
        else:
            pass

    # move the window to the center of screen
    def center(self):
        frameGm = self.frameGeometry()
        screen = QApplication.desktop().screenNumber(
            QApplication.desktop().cursor().pos())
        centerPoint = QApplication.desktop().screenGeometry(screen).center()
        frameGm.moveCenter(centerPoint)
        self.move(frameGm.topLeft())

    @pyqtSlot()
    def click_search(self):

        query_string = self.searchField.text().strip()

        # setup QMessageBox
        if query_string == '':
            self.__create_message_box('Please enter your query')
        else:
            search_result = self.search_engine.query(query_string)
            self.retrieved_doc_ids, corrections, scores = search_result.doc_id_lst, search_result.correction, search_result.result_scores

            if len(self.retrieved_doc_ids) == 0:
                self.__create_message_box('Could not find anything.')
            else:
                model = QStandardItemModel()
                for doc_id, score in zip(self.retrieved_doc_ids, scores):
                    doc_title = self.search_engine.get_doc_title(doc_id)
                    item = QStandardItem('[Score: %s] ' % round(score, 4) + doc_title)
                    model.appendRow(item)
                self.queryResult.setModel(model)
                self.queryResult.show()

            if corrections.correction_made():
                self.__create_message_box('Did you mean ... ?\n' + str(corrections))

    def changeChoiceState(self, button: QPushButton):
        if button.isChecked():
            if button.text() == BOOLEAN_MODEL_BUTTON_TEXT:
                self.search_engine.switch_model('boolean')
                print('Switched to boolean model')
            elif button.text() == VSM_MODEL_BUTTON_TEXT:
                self.search_engine.switch_model('vsm')
                print('Switched to vsm model')
            elif button.text() == 'UofO catalog':
                # # print('Radio button '%s' is clicked.' % button.text())
                # html_file = 'UofO_Courses.html'
                # if os.path.exists(CURRENT_DIR + '/../%s' % html_file):
                #     setup(html_file=CURRENT_DIR + '/../%s' % html_file)
                # else:
                #     raise Exception('Could not find '%s'' % html_file)
                pass
            elif button.text() == 'Reuters':
                pass

    def selectItem(self):
        selected_item_idx = self.queryResult.selectedIndexes()[0].row()
        # print('id selected: %d' % selected_item_idx)
        # print('doc title: %s' % self.retrieved_doc_ids[selected_item_idx])
        # print('doc content: %s' %
        #       self.search_engine.get_doc_content(self.retrieved_doc_ids[selected_item_idx]))
        selected_doc_id = self.retrieved_doc_ids[selected_item_idx]
        doc_content = self.search_engine.get_doc_content(selected_doc_id)
        doc_title = self.search_engine.get_doc_title(selected_doc_id)
        self.__display_course_details({'title': doc_title, 'content': doc_content})

    def __display_course_details(self, document):
        dialog = QDialog(parent=self)
        dialog.setWindowTitle('%s' % document.get('title'))
        dialog.resize(640, 480)
        vbox = QVBoxLayout()

        hbox1 = QHBoxLayout()
        titelLabel = QLabel('Title: \t')
        title = QLineEdit()
        title.setText(document.get('title'))
        title.setReadOnly(True)
        hbox1.addWidget(titelLabel)
        hbox1.addWidget(title)

        hbox2 = QHBoxLayout()
        contentLabel = QLabel('Content: ')
        content = QTextEdit()
        content.setReadOnly(True)
        content.setText(document.get('content'))
        hbox2.addWidget(contentLabel)
        hbox2.addWidget(content)

        vbox.addLayout(hbox1)
        vbox.addLayout(hbox2)

        dialog.setLayout(vbox)
        dialog.exec_()

    def __create_message_box(self, message):
        message_box = QMessageBox()
        message_box.setIcon(QMessageBox.Information)
        message_box.setText(message)
        # message_box.setWindowTitle('Search result message')
        message_box.setStandardButtons(QMessageBox.Ok)
        message_box.exec_()


if __name__ == '__main__':
    raw_src = '../UofO_Courses.html'
    # TODO
    app = QApplication(sys.argv)
    mainWindow = MainWindow()
    sys.exit(app.exec_())
