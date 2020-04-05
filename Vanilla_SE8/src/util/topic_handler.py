import pickle
from global_variable import TOPIC_INVERTED_INDEX


class TopicHandler:

    def __init__(self):
        self.topic_inverted_index = {}
        with open(TOPIC_INVERTED_INDEX, 'rb') as f:
            self.topic_inverted_index = pickle.load(f)

    def get_docids_with_topics(self, topic_list: list) -> set:
        doc_ids_lst = [self.topic_inverted_index[topic] for topic in topic_list]

        if len(doc_ids_lst) == 0:
            return set()

        r = set(doc_ids_lst[0])
        for i in range(1, len(topic_list)):
            r = r.union(r, set(doc_ids_lst[i]))
        return r

    def get_all_topics(self) -> list:
        all_topics = list(self.topic_inverted_index.keys())
        all_topics.sort()
        return all_topics


if __name__ == '__main__':
    th = TopicHandler()
    # lst = ['earn', 'acq']
    # print(th.get_docs_with_topics(lst))

    print(th.get_all_topics())
