import csv
import dictionary
import json
import text_process


class _Document:

    def __init__(self, doc_id, title, content):
        self.doc_id = doc_id
        self.title = title
        self.content = content


class Index:

    def __init__(self, df_dict, docid_tf_dict):
        self.df_dict = df_dict
        self.docid_tf_dict = docid_tf_dict

    def save(self, out):
        with open(out, 'w') as f:
            f.write(json.dumps({'DF_DICT': self.df_dict, 'DOCID_TF_DICT': self.docid_tf_dict}))

    @staticmethod
    def load(index_file):
        with open(index_file, 'r') as f:
            r = json.load(f)

        return Index(r['DF_DICT'], r['DOCID_TF_DICT'])

    def get(self, keyword: str) -> list:
        if keyword in self.docid_tf_dict.keys():
            return list(self.docid_tf_dict[keyword].keys())
        else:
            return []

def read_create_documents(corpus):
    docs = []
    with open(corpus, 'r') as corpus_file:
        reader = csv.reader(corpus_file)
        for row in reader:
            doc_id = row[0]
            title = row[1]
            content = row[2]
            docs.append(_Document(doc_id, title, content))
    return docs


def cal_tf_df(terms, documents):
    df_dict = {}  # k: term, v: df
    docid_tf_dict = {}  # k: term, v: postings (dict:{doc_id, tf})

    for term in terms:
        df_dict[term] = 0
        docid_tf_dict[term] = {}

    for doc in documents:
        doc_id = doc.doc_id
        search_field = ' '.join(text_process.process(doc.title + doc.content, True, True, True))
        for term in terms:
            count = search_field.count(term)
            if count > 0:
                df_dict[term] += 1
                docid_tf_dict[term][doc_id] = count

    return docid_tf_dict, df_dict


if __name__ == '__main__':
    corpus = '../course_corpus.csv'
    terms = dictionary.build_vocabulary(corpus)
    docs = read_create_documents(corpus)
    tf, df = cal_tf_df(terms, docs)


    # print(tf)
    # print({k: v for k, v in sorted(df.items(), key=lambda item: item[1], reverse=True)})

    idxfile = Index(df_dict = df, docid_tf_dict=tf)
    idxfile.save('INDEX')

    idxf2 = Index.load('INDEX')
    print(idxf2.get('plane'))