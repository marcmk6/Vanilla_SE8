import csv

from intermediate_class.document import _Document


class Corpus:

    def __init__(self, corpus_file: str):
        self.documents = {}
        self.corpus_id = 0 if 'course' in corpus_file else 1
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

    def __str__(self):
        return str(self.corpus_id)
