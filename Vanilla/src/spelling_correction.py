from math import sqrt
from strsimpy.jaro_winkler import JaroWinkler
from strsimpy.weighted_levenshtein import WeightedLevenshtein
from strsimpy.weighted_levenshtein import CharacterSubstitutionInterface


class SpellingCorrection:

    def __init__(self, mapping: dict):
        self.mapping = mapping

    def no_correction(self):
        return not self.mapping

    def __str__(self):
        return str(self.mapping)


# Idea from: https://stackoverflow.com/questions/29233888/edit-distance-such-as-levenshtein-taking-into-account-proximity-on-keyboard
KEYBOARD_COOR = {'q': {'x': 0, 'y': 0}, 'w': {'x': 1, 'y': 0}, 'e': {'x': 2, 'y': 0}, 'r': {'x': 3, 'y': 0},
                 't': {'x': 4, 'y': 0}, 'y': {'x': 5, 'y': 0}, 'u': {'x': 6, 'y': 0}, 'i': {'x': 7, 'y': 0},
                 'o': {'x': 8, 'y': 0}, 'p': {'x': 9, 'y': 0}, 'a': {'x': 0, 'y': 1}, 'z': {'x': 0, 'y': 2},
                 's': {'x': 1, 'y': 1}, 'x': {'x': 1, 'y': 2}, 'd': {'x': 2, 'y': 1}, 'c': {'x': 2, 'y': 2},
                 'f': {'x': 3, 'y': 1}, 'b': {'x': 4, 'y': 2}, 'm': {'x': 5, 'y': 2}, 'j': {'x': 6, 'y': 1},
                 'g': {'x': 4, 'y': 1}, 'h': {'x': 5, 'y': 1}, 'k': {'x': 7, 'y': 1},
                 'l': {'x': 8, 'y': 1}, 'v': {'x': 3, 'y': 2}, 'n': {'x': 5, 'y': 2}}


def _euc_distance(a, b):
    X_2 = (KEYBOARD_COOR[a]['x'] - KEYBOARD_COOR[b]['x']) ** 2
    Y_2 = (KEYBOARD_COOR[a]['y'] - KEYBOARD_COOR[b]['y']) ** 2
    return sqrt(X_2 + Y_2)


WEIGHTS = {}
for i in KEYBOARD_COOR.keys():
    for j in KEYBOARD_COOR.keys():
        WEIGHTS[(i, j)] = _euc_distance(i, j)


class CharacterSubstitution(CharacterSubstitutionInterface):
    def cost(self, c0, c1):
        if c0 in KEYBOARD_COOR.keys() and c1 in KEYBOARD_COOR.keys():
            return WEIGHTS[(c0, c1)]
        else:
            return 1


WEIGHTED_LEVENSHTEIN = WeightedLevenshtein(CharacterSubstitution())


# TODO: multiprocessing to speed up?
def get_closest_term(word: str, terms: list) -> str:
    scores = []
    for i, term in enumerate(terms):
        score = WEIGHTED_LEVENSHTEIN.distance(word, term)
        scores.append((score, term))
    scores = sorted(scores, key=lambda tpl: tpl[0])
    return scores[0][1]


JAROWINKLER = JaroWinkler()


# TODO: multiprocessing to speed up?
def _get_closest_term(word: str, terms: list) -> str:
    scores = []
    for i, term in enumerate(terms):
        score = JAROWINKLER.similarity(word, term)  # FIXME order? proper way?
        scores.append((score, term))
    scores = sorted(scores, key=lambda tpl: tpl[0], reverse=True)
    return scores[0][1]
