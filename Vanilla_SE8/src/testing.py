from search_engine import SearchEngine


def run_test():
    boolean_retrieval_test_query = ['(*ge AND_NOT (man* OR health*))',
                                    '(statistical OR su*ort)',
                                    '(operating AND (system OR platform))',
                                    '(query AND processing)',
                                    'ps*logy',
                                    'leadership']

    vsm_retrieval_test_query = ['operoting system',
                                'computers graphical',
                                'lienar',
                                'business administration',
                                'child psychology',
                                'bayesian network classification']

    se = SearchEngine(model='boolean')
    se.load_index()
    for q in boolean_retrieval_test_query:
        r = se.query(q)
        lst, correction = r.doc_id_lst, r.correction
        print('Query: %s' % (q))
        print(lst[:5])
        print(correction)
    se.switch_model()
    for q in vsm_retrieval_test_query:
        r = se.query(q)
        lst, correction = r.doc_id_lst, r.correction
        print('Query: %s' % (q))
        print(lst[:5])
        print(correction)

    print()
    print(se.get_doc_excerpt('CSI_4107'))


if __name__ == '__main__':
    run_test()
