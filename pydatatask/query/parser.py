from typing import List, Union
from dataclasses import dataclass

import ply.lex as lex
import ply.yacc as yacc

# Token definitions
tokens = (
    "STRING_LITERAL",
    "INT_LITERAL",
    "BOOL_LITERAL",
    "KEY_LITERAL",
    "IDENTIFIER",
    "OR",
    "AND",
    "PLUS",
    "MINUS",
    "TIMES",
    "DIVIDE",
    "MODULUS",
    "AMPERSAND",
    "CARET",
    "PIPE",
    "EQ",
    "NEQ",
    "GT",
    "LT",
    "GEQ",
    "LEQ",
    "LPAREN",
    "RPAREN",
    "LBRACKET",
    "RBRACKET",
    "LBRACE",
    "RBRACE",
    "COMMA",
    "DOT",
    "NOT",
    "TILDE",
)

# Regular expressions for tokens
t_STRING_LITERAL = r'"[^"]*"'
t_INT_LITERAL = r"[+-]?\d+"
t_BOOL_LITERAL = r"true|false"
t_KEY_LITERAL = r"`[a-zA-Z0-9_]*`"
t_OR = r"\|\|"
t_AND = r"&&"
t_PLUS = r"\+"
t_MINUS = r"-"
t_TIMES = r"\*"
t_DIVIDE = r"/"
t_MODULUS = r"%"
t_AMPERSAND = r"&"
t_CARET = r"\^"
t_PIPE = r"\|"
t_EQ = r"=="
t_NEQ = r"!="
t_GT = r">"
t_LT = r"<"
t_GEQ = r">="
t_LEQ = r"<="
t_LPAREN = r"\("
t_RPAREN = r"\)"
t_LBRACKET = r"\["
t_RBRACKET = r"\]"
t_LBRACE = r"\{"
t_RBRACE = r"\}"
t_COMMA = r","
t_DOT = r"\."
t_NOT = r"!"
t_TILDE = r"~"
t_ignore = " \t"


def t_IDENTIFIER(t):
    r"[a-zA-Z_][a-zA-Z0-9_]*"
    t.type = "IDENTIFIER"
    return t


def t_newline(t):
    r"\n+"
    t.lexer.lineno += len(t.value)


def t_error(t):
    print(f"Illegal character '{t.value[0]}'")
    t.lexer.skip(1)


lexer = lex.lex()

# Define precedence and associativity
precedence = (
    ("left", "OR"),
    ("left", "AND"),
    ("left", "PIPE"),
    ("left", "CARET"),
    ("left", "AMPERSAND"),
    ("left", "EQ", "NEQ"),
    ("left", "LT", "LEQ", "GT", "GEQ"),
    ("left", "PLUS", "MINUS"),
    ("left", "TIMES", "DIVIDE", "MODULUS"),
    ("right", "UMINUS", "NOT", "TILDE"),
    ("left", "LBRACKET", "RBRACKET"),
)

# Grammar rules
def p_expr_name(p):
    "expr : IDENTIFIER"
    p[0] = IdentExpression(p[1])


def p_expr_stringlit(p):
    """expr : STRING_LITERAL"""
    p[0] = StringLiteral(p[1][1:-1])


def p_expr_intlit(p):
    """expr : INT_LITERAL"""
    p[0] = IntLiteral(int(p[1], 0))


def p_expr_boollit(p):
    """expr : BOOL_LITERAL"""
    p[0] = BoolLiteral(p[1] == "true")


def p_expr_keylit(p):
    """expr : KEY_LITERAL"""
    p[0] = KeyLiteral(p[1][1:-1])


def p_expr_list(p):
    """expr : LBRACE args RBRACE"""
    p[0] = ListLiteral(p[2])


def p_expr_index(p):
    """expr : expr LBRACKET expr RBRACKET"""
    p[0] = FunctionCall(FuncExpr("__index__", []), [p[1], p[3]])


def p_expr_binop(p):
    """expr : expr OR expr
    | expr AND expr
    | expr PLUS expr
    | expr MINUS expr
    | expr TIMES expr
    | expr DIVIDE expr
    | expr MODULUS expr
    | expr AMPERSAND expr
    | expr CARET expr
    | expr PIPE expr
    | expr EQ expr
    | expr NEQ expr
    | expr GT expr
    | expr LT expr
    | expr GEQ expr
    | expr LEQ expr"""
    p[0] = FunctionCall(FuncExpr(BINOP_TOKEN_MAPPING[p[2]], []), [p[1], p[3]])


def p_expr_unop(p):
    """expr : NOT expr
    | TILDE expr
    | PLUS expr %prec UMINUS
    | MINUS expr %prec UMINUS"""
    p[0] = FunctionCall(FuncExpr(UNOP_TOKEN_MAPPING[p[1]], []), [p[2]])


def p_expr_parentheses(p):
    "expr : LPAREN expr RPAREN"
    p[0] = p[2]


def p_expr_function_call(p):
    "expr : funcexpr LPAREN args RPAREN"
    p[0] = FunctionCall(p[1], p[3])


def p_expr_method_call(p):
    "expr : expr funcexpr LPAREN args RPAREN"
    p[0] = FunctionCall(p[2], [p[1]] + p[4])


def p_funcexpr_template(p):
    "funcexpr : DOT IDENTIFIER LBRACKET tempargs RBRACKET"
    p[0] = FuncExpr(p[2], p[4])


def p_funcexpr_notemplate(p):
    "funcexpr : DOT IDENTIFIER"
    p[0] = FuncExpr(p[2], [])


def p_args_empty(p):
    "args :"
    p[0] = []


def p_args_one(p):
    "args : expr"
    p[0] = [p[1]]


def p_args_multiple(p):
    "args : expr COMMA args"
    p[0] = [p[1]] + p[3]


def p_tempargs_empty(p):
    "tempargs :"
    p[0] = []


def p_tempargs_one(p):
    "tempargs : tempexpr"
    p[0] = [p[1]]


def p_tempargs_multiple(p):
    "tempargs : tempexpr COMMA tempargs"
    p[0] = [p[1]] + p[3]


def p_tempexpr(p):
    """
    tempexpr : funcexpr
             | expr
    """
    p[0] = p[1]


def p_error(p):
    print(f"Syntax error at line {p.lineno}, position {p.lexpos}: Unexpected token '{p.value}'")


BINOP_TOKEN_MAPPING = {
    "+": "__add__",
    "-": "__sub__",
    "*": "__mul__",
    "/": "__div__",
    "%": "__mod__",
    "&": "__bitand__",
    "|": "__bitor__",
    "^": "__bitxor__",
    "&&": "__logand__",
    "||": "__logor__",
    "==": "__eq__",
    "!=": "__ne__",
    ">": "__gt__",
    "<": "__lt__",
    ">=": "__ge__",
    "<=": "__le__",
}


UNOP_TOKEN_MAPPING = {
    "~": "__inv__",
    "!": "__not__",
    "-": "__neg__",
    "+": "__plus__",
}


@dataclass
class Expression:
    pass


@dataclass
class IdentExpression(Expression):
    name: str


@dataclass
class StringLiteral(Expression):
    value: str


@dataclass
class KeyLiteral(Expression):
    value: str


@dataclass
class IntLiteral(Expression):
    value: int


@dataclass
class BoolLiteral(Expression):
    value: bool


@dataclass
class FuncExpr:
    name: str
    template_args: List[Union["FuncExpr", Expression]]


@dataclass
class FunctionCall(Expression):
    function: FuncExpr
    args: List[Expression]


@dataclass
class ListLiteral(Expression):
    values: List[Expression]


parser = yacc.yacc()

if __name__ == "__main__":
    # Test the parser
    while True:
        input_expr = input()
        if not input_expr:
            break
        result = parser.parse(input_expr)
        print(result)
