import re
import text_processing
from index import Index, bigrams_2_terms
import wildcard_handler


def _is_operand(exp: str) -> bool:
    return re.search(r'\(|\)|AND|OR|AND_NOT', exp) is None


def _is_wildcard_query_operand(operand: str) -> bool:
    return re.search(r'\*', operand) is not None


# ref: https://runestone.academy/runestone/books/published/pythonds/BasicDS/InfixPrefixandPostfixExpressions.html
def infix_2_postfix(infix_expr: str) -> str:
    precedence = {'OR': 1, 'AND': 2, 'AND_NOT': 3, '(': 0}

    operator_stack = []
    postfix_expr = []

    tokens_str = ''
    for t in infix_expr.split():
        t = re.sub(r'\(', '( ', t)
        t = re.sub(r'\)', ' )', t)
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


def equivalences_2_query(equivalent_words: set, original_wildcard_query: str) -> str:
    regex = re.sub(r'\*', '[a-zA-Z]*', original_wildcard_query)
    t = set()
    for word in equivalent_words:
        if re.search(regex, original_wildcard_query) is not None:
            t.add(word)
    return '( ' + ' OR '.join(t) + ' )'


def query(idx: Index, raw_query: str) -> list:
    tmp = []
    for t in raw_query.split():
        if _is_operand(t):
            if _is_wildcard_query_operand(t):
                bigrams = wildcard_handler.get_bigrams(t)
                unchecked_equivalences = bigrams_2_terms(idx, bigrams)
                t = equivalences_2_query(unchecked_equivalences, t)
            else:
                t = text_processing.process(t)[0]
        tmp.append(t)

    processed_query = ' '.join(tmp)
    postfix_expr_tokens = infix_2_postfix(processed_query).split()

    # Single word query
    if len(postfix_expr_tokens) == 1:
        return idx.get(postfix_expr_tokens.pop())

    operand_stack = []
    result = []
    for expr_token in postfix_expr_tokens:

        if _is_operand(expr_token):  # token is keyword
            operand_stack.append(expr_token)

        else:  # token is bool operator

            if type(operand_stack[-1]) is str:
                operand_2 = idx.get(operand_stack.pop())
            else:
                operand_2 = operand_stack.pop()

            if type(operand_stack[-1]) is str:
                operand_1 = idx.get(operand_stack.pop())
            else:
                operand_1 = operand_stack.pop()

            result = perform_bool_operation(expr_token, operand_1, operand_2)
            operand_stack.append(result)

    return result


if __name__ == "__main__":
    # print(infix_2_postfix("(*ge AND_NOT (man* OR health*))"))
    # print(and_not_operation([1, 2, 3], [1, 2, 4, 35]))

    idxf2 = Index._load('idx_full')

    print(query(idxf2, '*hood'))
