import re

from util import text_processing
from index_v2 import Index_v2, bigrams_2_terms
from intermediate_class.search_result import SearchResult
from util.wildcard_handler import get_bigrams
from util.spelling_correction import SpellingCorrection, get_closest_term
from global_variable import DUMMY_WORD, UNFOUND_TERM_LIMIT


def _is_operand(exp: str) -> bool:
    operators = ['(', ')', 'AND', 'OR', 'AND_NOT']
    return exp not in operators


def _is_wildcard_query_operand(operand: str) -> bool:
    return '*' in operand


# ref: https://runestone.academy/runestone/books/published/pythonds/BasicDS/InfixPrefixandPostfixExpressions.html
def infix_2_postfix(infix_expr: str) -> str:
    precedence = {'OR': 1, 'AND': 2, 'AND_NOT': 3, '(': 0}

    operator_stack = []
    postfix_expr = []

    tokens_str = ''
    for t in infix_expr.split():
        t = t.replace('(', '( ')
        t = t.replace(')', ' )')
        tokens_str += t + ' '
    tokens = tokens_str.split()

    for token in tokens:
        if _is_operand(token):
            postfix_expr.append(token)
        elif token == '(':
            operator_stack.append(token)
        elif token == ')':
            top_token = operator_stack.pop()
            while top_token != '(':
                postfix_expr.append(top_token)
                top_token = operator_stack.pop()
        else:
            while (len(operator_stack) != 0) and (precedence[operator_stack[-1]] >= precedence[token]):
                postfix_expr.append(operator_stack.pop())
            operator_stack.append(token)

    while not len(operator_stack) == 0:
        postfix_expr.append(operator_stack.pop())
    return ' '.join(postfix_expr)


def and_operation(l1: list, l2: list) -> list:
    result = []
    ptr_1, ptr_2 = 0, 0
    while ptr_1 < len(l1) and ptr_2 < len(l2):
        if l1[ptr_1] == l2[ptr_2]:
            result.append(l1[ptr_1])
            ptr_1 += 1
            ptr_2 += 1
        elif l1[ptr_1] > l2[ptr_2]:
            ptr_2 += 1
        else:
            ptr_1 += 1

    return result


def or_operation(l1: list, l2: list) -> list:
    result = []
    for e in l1:
        result.append(e)
    for e in l2:
        if e not in result:
            result.append(e)
    return result


def and_not_operation(l1: list, l2: list) -> list:
    result = []
    ptr_1, ptr_2 = 0, 0
    while ptr_1 < len(l1) and ptr_2 < len(l2):

        if l1[ptr_1] > l2[ptr_2]:
            ptr_2 += 1
        elif l1[ptr_1] == l2[ptr_2]:
            ptr_1 += 1
            ptr_2 += 1
        else:
            result.append(l1[ptr_1])
            ptr_1 += 1

    if ptr_1 < len(l1):
        while ptr_1 < len(l1):
            result.append(l1[ptr_1])
            ptr_1 += 1

    return result


def perform_bool_operation(operator: str, operand_1: list, operand_2: list) -> list:
    if operator == 'OR':
        r = or_operation(operand_1, operand_2)
    elif operator == 'AND':
        r = and_operation(operand_1, operand_2)
    elif operator == 'AND_NOT':
        r = and_not_operation(operand_1, operand_2)
    return r


# FIXME problem?
def equivalences_2_query(equivalent_words: set, original_wildcard_query: str) -> str:
    regex = original_wildcard_query.replace('*', '[a-zA-Z]*')
    t = set()
    for word in equivalent_words:
        if re.search(regex, word) is not None:
            t.add(word)
    return '( ' + ' OR '.join(t) + ' )'


def query(index: Index_v2, raw_query: str) -> SearchResult:
    raw_query = raw_query.replace('(', '( ')
    raw_query = raw_query.replace(')', ' )')

    tmp = []
    for t in raw_query.split():
        if _is_operand(t):
            if _is_wildcard_query_operand(t):
                bigrams = get_bigrams(t)
                unchecked_equivalences = bigrams_2_terms(index, bigrams)
                t = equivalences_2_query(unchecked_equivalences, t)
            else:
                t = text_processing.process(string=t, config=index.config)[0]
        tmp.append(t)

    processed_query = ' '.join(tmp)
    processed_query = re.sub(r'\(\s+\)', '( ' + DUMMY_WORD + ' )', processed_query)
    postfix_expr_tokens = infix_2_postfix(processed_query).split()

    # Spelling correction
    spelling_correction_obj = SpellingCorrection(mapping={})
    correction_candidates = []
    for idx, tkn in enumerate(postfix_expr_tokens):
        if _is_operand(tkn) and tkn not in index.terms and tkn != DUMMY_WORD:
            correction_candidates.append([idx, tkn, get_closest_term(word=tkn, terms=index.terms)])
    correction_made = sorted(correction_candidates, key=lambda x: index.get_total_term_frequency(x[2]), reverse=True)[
                      :UNFOUND_TERM_LIMIT]
    for e in correction_made:
        idx = e[0]
        old_term = e[1]
        correction = e[2]
        postfix_expr_tokens[idx] = correction
        spelling_correction_obj.mapping[old_term] = correction

    # Single word query
    if len(postfix_expr_tokens) == 1:
        retrieved_doc_ids = index.get(postfix_expr_tokens.pop())
        retrieved_doc_ids = [str(e) for e in retrieved_doc_ids]
        tmp = SearchResult(doc_id_list=retrieved_doc_ids, correction=spelling_correction_obj,
                           result_scores=[1] * len(retrieved_doc_ids))
        return tmp

    operand_stack = []
    result = []
    for expr_token in postfix_expr_tokens:

        if _is_operand(expr_token):  # token is keyword
            operand_stack.append(expr_token)

        else:  # token is bool operator

            if type(operand_stack[-1]) is str:
                operand_2 = index.get(operand_stack.pop())
            else:
                operand_2 = operand_stack.pop()

            if type(operand_stack[-1]) is str:
                operand_1 = index.get(operand_stack.pop())
            else:
                operand_1 = operand_stack.pop()

            result = perform_bool_operation(expr_token, operand_1, operand_2)
            operand_stack.append(result)

    result = [str(e) for e in result]
    search_result = SearchResult(doc_id_list=result, correction=spelling_correction_obj,
                                 result_scores=[1] * len(result))
    return search_result
