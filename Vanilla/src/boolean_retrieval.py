import re
import index


def _is_operand(exp: str) -> bool:
    return re.search('\(|\)|AND|OR|AND_NOT', exp) is None


# ref: https://runestone.academy/runestone/books/published/pythonds/BasicDS/InfixPrefixandPostfixExpressions.html
def infix_2_postfix(infix_expr) -> str:
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
            while (not len(operator_stack) == 0) and (precedence[operator_stack[-1]] >= precedence[token]):
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


def get_docid(idx: index, keyword: str):
    return idx.get(keyword)


def perform_bool_operation(operator: str, operand_1: list, operand_2: list) -> list:
    if operator == 'OR':
        r = or_operation(operand_1, operand_2)
    elif operator == 'AND':
        r = and_operation(operand_1, operand_2)
    elif operator == 'AND_NOT':
        r = and_not_operation(operand_1, operand_2)
    return r


def eval(idx: index, query: str) -> list:
    postfix_expr_tokens = infix_2_postfix(query).split()

    # Single word query
    if len(postfix_expr_tokens) == 1:
        return idx.get(postfix_expr_tokens.pop())

    operand_stack = []
    r = []
    for expr_token in postfix_expr_tokens:

        if _is_operand(expr_token):  # token is keyword
            operand_stack.append(expr_token)

        else:   # token is bool operator

            if type(operand_stack[-1]) is str:
                operand_2 = idx.get(operand_stack.pop())
            else:
                operand_2 = operand_stack.pop()

            if type(operand_stack[-1]) is str:
                operand_1 = idx.get(operand_stack.pop())
            else:
                operand_1 = operand_stack.pop()

            r = perform_bool_operation(expr_token, operand_1, operand_2)
            operand_stack.append(r)

    return r


if __name__ == "__main__":
    # print(infix_2_postfix("(*ge AND_NOT (man* OR health*))"))
    # print(and_not_operation([1, 2, 3], [1, 2, 4, 35]))

    idxf2 = index.Index.load('INDEX')

    print(eval(idxf2, 'algorithm AND plane'))
