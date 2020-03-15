from util.spelling_correction import SpellingCorrection


class SearchResult:

    def __init__(self, doc_id_list: list, correction: SpellingCorrection, result_scores: list):
        self.doc_id_list = doc_id_list
        self.correction = correction
        self.result_scores = result_scores

    def filter_by_doc_ids(self, selected_doc_id_range: set):
        remaining_idx = []
        remaining_ids = []
        for i, doc_id in enumerate(self.doc_id_list):
            if doc_id in selected_doc_id_range:
                remaining_idx.append(i)
                remaining_ids.append(doc_id)
        self.doc_id_list = remaining_ids
        self.result_scores = [self.result_scores[i] for i in remaining_idx]
