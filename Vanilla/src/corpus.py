from bs4 import BeautifulSoup
import csv
import re
from index import _Document

COURSE_HTML = '../UofO_Courses.html'
COURSE_CORPUS_OUTPUT = '../course_corpus_full.csv'


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

    # FIXME
    def get_doc_excerpt(self, doc_id: str) -> str:
        return self.documents[doc_id].content[:50]

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

        with open(COURSE_CORPUS_OUTPUT, 'a') as o:
            writer = csv.writer(o)
            for doc_id, name, course_description in zip(doc_id_lst, course_name, course_description_lst):
                writer.writerow([doc_id, name, course_description])

    files = _separate_files(COURSE_HTML)
    for file in files:
        _preprocess_course_corpus(file)
