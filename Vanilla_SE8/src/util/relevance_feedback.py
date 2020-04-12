import numpy as np

from global_variable import ALPHA, BETA, GAMMA


class RelevanceFeedback:
    def __init__(self, query_vec: np.ndarray, p_vec_list: list, n_vec_list: list):
        # list of np.ndarray
        self.query_vec = query_vec
        self.positive_doc_vec_list = p_vec_list
        self.negative_doc_vec_list = n_vec_list
        self.p_doc_count = len(self.positive_doc_vec_list)
        self.n_doc_count = len(self.negative_doc_vec_list)
        self.p_doc_vec_sum = self.__get_vec_sum__(self.positive_doc_vec_list)
        self.n_doc_vec_sum = self.__get_vec_sum__(self.negative_doc_vec_list)

        self.expanded_query_vec = self.__rocchio__(q0=query_vec, p_doc_count=self.p_doc_count,
                                                   n_doc_count=self.n_doc_count,
                                                   p_doc_vec_sum=self.p_doc_vec_sum,
                                                   n_doc_vec_sum=self.n_doc_vec_sum)

    def get_expanded_query(self):
        return self.expanded_query_vec

    @staticmethod
    def merge(rf1, rf2):
        assert np.array_equal(rf1.query_vec, rf2.query_vec)
        p_vec_list = rf1.positive_doc_vec_list + rf2.positive_doc_vec_list
        n_vec_list = rf1.negative_doc_vec_list + rf2.negative_doc_vec_list
        return RelevanceFeedback(rf1.query_vec, p_vec_list, n_vec_list)

    @staticmethod
    def __get_vec_sum__(vec_list: list) -> np.ndarray:
        if len(vec_list) > 0:
            summation = np.zeros(vec_list[0].shape, dtype=vec_list[0].dtype)
            for vec in vec_list:
                summation += vec
            return summation
        return np.asarray([])

    @staticmethod
    def __rocchio__(q0: np.ndarray, p_doc_count: int, n_doc_count: int,
                    p_doc_vec_sum: np.ndarray, n_doc_vec_sum: np.ndarray) -> np.ndarray:
        term1 = ALPHA * q0
        term2 = BETA * (1 / p_doc_count) * p_doc_vec_sum if p_doc_count != 0 else 0
        term3 = GAMMA * (1 / n_doc_count) * n_doc_vec_sum if n_doc_count != 0 else 0
        return term1 + term2 - term3


class RelevanceFeedbackSession:

    def __init__(self):
        self.relevance_memory = {}  # k: query: str, v: RelevanceFeedback obj

    def add_relevance_feedback(self, query: str, rf: RelevanceFeedback):
        if query not in self.relevance_memory.keys():
            self.relevance_memory[query] = rf
        else:
            existing = self.relevance_memory[query]
            self.relevance_memory[query] = RelevanceFeedback.merge(existing, rf)

    def exists_rf(self, query: str):
        return query in self.relevance_memory.keys()

    def get_expanded_query(self, query: str) -> np.ndarray:
        return self.relevance_memory[query].get_expanded_query()
