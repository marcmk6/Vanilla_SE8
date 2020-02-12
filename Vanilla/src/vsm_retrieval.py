import numpy as np
import text_processing
from index import Index


def vectorize_query(idx: Index, raw_query: str) -> np.ndarray:
    tokens = []
    for t in raw_query.split():
        tokens.append(text_processing.process(t)[0])

    terms = list(idx.df_dict.keys())
    vectorized_query = [0] * len(terms)
    for tk in tokens:
        if tk in terms:
            idx = terms.index(tk)
            vectorized_query[idx] += 1

    return np.asarray(vectorized_query)



if __name__ == '__main__':
    idx = Index.load('INDEX')

    print(vectorize_query(idx=idx, raw_query='linear time'))