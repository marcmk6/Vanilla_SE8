from bs4 import BeautifulSoup
import csv
import re
import os
from index import _Document

COURSE_HTML = '../UofO_Courses.html'
COURSE_CORPUS_OUTPUT = '../course_corpus_full.csv'
REUTERS_SOURCE_DIR = '../corpus/reuters21578/'
REUTERS_CORPUS_OUTPUT = '../corpus/reuters_corpus.csv'

class Corpus:

    def __init__(self, corpus_file: str):
        self.documents = {}
        with open(corpus_file, 'r') as f:
            reader = csv.reader(f)
            for row in reader:
                doc_id = row[0]
                title = row[1]
                content = row[2]
                self.documents[doc_id] = (_Document(doc_id, title, content))

    def get_doc_excerpt(self, doc_id: str) -> str:
        """Return the first sentence as the excerpt"""
        content = self.documents[doc_id].content
        return content[:content.find('.') + 1]

    def get_doc_title(self, doc_id: str) -> str:
        return self.documents[doc_id].title

    def get_doc_content(self, doc_id: str) -> str:
        return self.documents[doc_id].content


def preprocess_course_corpus():
    def _separate_files(src_file):
        end = '</html>'
        with open(src_file, 'r') as f:
            files = f.read().split(end)
        return files

    def _preprocess_course_corpus(src_file):
        soup = BeautifulSoup(src_file, 'lxml')

        course_name = []
        course_description_lst = []
        doc_id_lst = []

        for e in soup.find_all('div', class_='courseblock'):
            name = e.find('p', class_='courseblocktitle noindent')
            course_description = e.find('p', class_='courseblockdesc noindent')

            if name is not None:
                name = name.text
                if 1 <= int(name[5]) <= 3:  # english course
                    course_name.append(name)

                    # docid = name[:8]
                    doc_id = re.sub(' ', '_', name[:8])
                    doc_id_lst.append(doc_id)

                    if course_description is not None:
                        course_description_lst.append(course_description.text.strip('\n'))
                    else:
                        course_description_lst.append('')

        with open(COURSE_CORPUS_OUTPUT, 'a', newline='') as o:
            writer = csv.writer(o)
            for doc_id, name, course_description in zip(doc_id_lst, course_name, course_description_lst):
                writer.writerow([doc_id, name, course_description])

    files = _separate_files(COURSE_HTML)
    for file in files:
        _preprocess_course_corpus(file)


def preprocess_reuters_corpus():

    target_files = [f for f in os.listdir(REUTERS_SOURCE_DIR) if f.endswith('3.sgm')]
    target_files = sorted(target_files)

    # Extract information
    doc_ids = []
    contents = []
    topics_lst = []
    titles = []
    for target_file in target_files:
        with open(REUTERS_SOURCE_DIR + target_file, 'r', encoding='ISO-8859-1') as f:
            src_file = f.read()
        soup = BeautifulSoup(src_file, 'html.parser')
        for e in soup.find_all('reuters'):
            doc_id = e.get('newid')
            content = e.find('text')
            main_content = (lambda x: x.string if x is not None else '')(content.find('body'))
            topics = (lambda x: list(map(lambda y: y.string, x)) if x != [] else x)(e.find('topics').find_all('d'))
            title = (lambda x: x.string if x is not None else '')(content.find('title'))

            doc_ids.append(doc_id)
            contents.append(main_content.strip())
            topics_lst.append(topics)
            titles.append(title)

    # Write corpus
    with open(REUTERS_CORPUS_OUTPUT, 'w', newline='') as o:
        writer = csv.writer(o)
        for doc_id, title, content, topics in zip(doc_ids, titles, contents, topics_lst):
            writer.writerow([doc_id, title, content, topics])
