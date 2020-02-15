from bs4 import BeautifulSoup
import csv
import re
from index import _Document

SRC = '../UofO_Courses.html'
OUT = '../course_corpus_full.csv'
CORPUS = OUT


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
    def get_doc_excerpt(self, doc_id:str) -> str:
        return self.documents[doc_id].content[:50]

    def get_doc_title(self, doc_id: str) -> str:
        return self.documents[doc_id].title

    def get_doc_content(self, doc_id: str) -> str:
        return self.documents[doc_id].content


def _separate_files(src_file):
    end = '</html>'
    with open(src_file, 'r') as f:
        files = f.read().split(end)

    return files


def preprocess_course_corpus(src_file):
    soup = BeautifulSoup(src_file, 'lxml')

    course_name = []
    course_description = []
    doc_id = []

    for e in soup.find_all('div', class_='courseblock'):
        name = e.find('p', class_='courseblocktitle noindent')
        course_description = e.find('p', class_='courseblockdesc noindent')

        if name is not None:
            name = name.text
            if 1 <= int(name[5]) <= 3:  # english course
                course_name.append(name)

                # docid = name[:8]
                docid = re.sub(' ', '_', name[:8])
                doc_id.append(docid)

                if course_description is not None:
                    course_description.append(course_description.text.strip('\n'))
                else:
                    course_description.append('')

    with open(OUT, 'a') as o:
        writer = csv.writer(o)
        for docid, name, course_description in zip(doc_id, course_name, course_description):
            writer.writerow([docid, name, course_description])

# TODO: remove
# if __name__ == "__main__":
#
#     """
#     files = _separate_files(SRC)
#     for file in files:
#         preprocess_course_corpus(file)
#     """
#     # preprocess(files[3])
#
#     cp = Corpus(OUT)
#     print(cp.get_doc_excerpt('CSI_4107'))
