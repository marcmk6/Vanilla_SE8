from strsimpy.jaro_winkler import JaroWinkler

JAROWINKLER = JaroWinkler()


class SpellingCorrection:

    def __init__(self, mapping: dict):
        self.mapping = mapping

    def no_correction(self):
        return not self.mapping  # mapping is empty

    def __str__(self):
        return str(self.mapping)


# TODO: multiprocessing to speed up?
def get_closest_term(word: str, terms: list) -> str:
    scores = []
    for i, term in enumerate(terms):
        score = JAROWINKLER.similarity(word, term)  # FIXME order? proper way?
        scores.append((score, term))
    scores = sorted(scores, key=lambda tpl: tpl[0], reverse=True)
    return scores[0][1]
