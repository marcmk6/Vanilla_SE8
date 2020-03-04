import re


def get_bigrams(word: str) -> set:
    chars = ['$'] + [char for char in word if char != '*'] + ['$']
    bigrams = []
    for i in range(0, len(chars) - 1):
        bigrams.append(chars[i] + chars[i + 1])
    if word[0] == '*':
        bigrams.pop(0)
    elif word[-1] == '*':
        bigrams.pop()
    elif word.find('*') == -1:
        pass
    else:
        bigrams.pop(word.find('*'))
    return set(bigrams)


def bigram_2_regex(bigram: str) -> str:
    """DEPRECATED"""
    other_chars = '[a-zA-Z]+'
    if bigram[0] == '$':
        bigram = re.sub('\$', '', bigram)
        return '^' + bigram + other_chars
    elif bigram[-1] == '$':
        return other_chars + bigram
    else:
        return bigram


def bigram_term_matched(bigram: str, term: str) -> bool:
    if bigram[0] == '$':
        return term[0] == bigram[1]
    elif bigram[-1] == '$':
        return term[-1] == bigram[-2]
    else:
        return bigram in term
