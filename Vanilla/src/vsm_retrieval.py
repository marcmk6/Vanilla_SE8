import numpy as np
import text_processing
from index import Index

DOC_RETRIEVAL_LIMIT = 50


def vectorize_query(index: Index, raw_query: str) -> np.ndarray:
    """
    :param index:
    :param raw_query:
    :return: ndarray (v,)
    """
    tokens = []
    for t in raw_query.split():
        tokens.append(text_processing.process(t)[0])

    terms = list(index.df_dict.keys())
    vectorized_query = [0] * len(terms)
    for tk in tokens:
        if tk in terms:
            index = terms.index(tk)
            vectorized_query[index] += 1
    return np.asarray(vectorized_query)


def query(vectorized_query: np.ndarray, index: Index) -> list:
    ranking = np.dot(index.tf_idf_matrix, vectorized_query).tolist()
    full_results = []
    for i, score in enumerate(ranking):
        if score != 0:
            doc_id = index.doc_ids['DOC_ID'][i]
            pair = (doc_id, score)
            full_results.append(pair)

    full_results = sorted(full_results, key=lambda tpl: tpl[1])
    top_results = []
    for i in range(0, min(len(full_results), DOC_RETRIEVAL_LIMIT)):
        top_results.append(full_results[i][0])
    return top_results


if __name__ == '__main__':
    idx = Index.load('INDEX')

    # print(idx.df_dict)
    vec = vectorize_query(index=idx, raw_query='the')
    print(query(vec, idx))
    pass
