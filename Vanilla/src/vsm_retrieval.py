import numpy as np
import text_processing
from index import Index, UNFOUND_TERM_LIMIT
from spelling_correction import SpellingCorrection, get_closest_term

DOC_RETRIEVAL_LIMIT = 50


def _vectorize_query(index: Index, raw_query: str) -> (np.ndarray, SpellingCorrection):
    """
    Vectorize query
    If there is any unfound term, perform spelling correction
    :param index:
    :param raw_query:
    :return: (ndarray (v,), spelling_correction_obj)
    """
    tokens = []
    for t in raw_query.split():
        tokens.append(text_processing.process(string=t, config=index.config)[0])

    # terms = list(index.df_dict.keys())
    terms = index.terms
    vectorized_query = [0] * len(terms)

    terms_not_found = []
    for tk in tokens:
        if tk in terms:
            i = terms.index(tk)
            vectorized_query[i] += 1
        else:
            terms_not_found.append(tk)

    # Spelling correction
    spelling_correction_obj = SpellingCorrection(mapping={})
    if len(terms_not_found) != 0:

        unfounded_terms_correction = []
        for term_not_found in terms_not_found:
            correction = get_closest_term(word=term_not_found, terms=terms)
            unfounded_terms_correction.append((term_not_found, correction))

        # Take top N most likely candidates
        unfounded_terms_correction = sorted(unfounded_terms_correction, key=lambda x: index.get_term_frequency(x[1]),
                                            reverse=True)[:UNFOUND_TERM_LIMIT]

        for unfound_term, correction in unfounded_terms_correction:
            spelling_correction_obj.mapping[unfound_term] = correction
            # Add correction back to query vector
            index = terms.index(correction)
            vectorized_query[index] += 1

    return np.asarray(vectorized_query), spelling_correction_obj


def query(index: Index, query: str) -> (list, SpellingCorrection):
    vectorized_query, spelling_correction_obj = _vectorize_query(index, query)
    ranking = np.dot(index.tf_idf_matrix, vectorized_query).tolist()
    full_results = []
    for i, score in enumerate(ranking):
        if score != 0:
            doc_id = index.doc_ids[i]
            pair = (doc_id, score)
            full_results.append(pair)

    full_results = sorted(full_results, key=lambda tpl: tpl[1], reverse=True)
    top_results = []
    for i in range(0, min(len(full_results), DOC_RETRIEVAL_LIMIT)):
        top_results.append(full_results[i][0])
    return top_results, spelling_correction_obj

# TODO: remove
# if __name__ == '__main__':
#     idx = Index._load('idx_full')
#
#     # print(idx.df_dict)
#     print(query(idx, 'information system management'))
#     pass
