import ply.lex as lex
import ply.yacc as yacc

# Token definitions
tokens = (
    'STRING_LITERAL',
    'INT_LITERAL',
    'BOOL_LITERAL',
    'IDENTIFIER',
    'PLUS',
    'MINUS',
    'TIMES',
    'DIVIDE',
    'AMPERSAND',
    'CARET',
    'PIPE',
    'EQ',
    'NEQ',
    'GT',
    'LT',
    'GEQ',
    'LEQ',
    'LPAREN',
    'RPAREN',
    'LBRACKET',
    'RBRACKET',
    'LBRACE',
    'RBRACE',
    'COMMA',
    'DOT',
    'NOT',
    'TILDE',
)

# Regular expressions for tokens
t_STRING_LITERAL = r'"[^"]*"'
t_INT_LITERAL = r'[+-]?\d+'
t_BOOL_LITERAL = r'true|false'
t_PLUS = r'\+'
t_MINUS = r'-'
t_TIMES = r'\*'
t_DIVIDE = r'/'
t_AMPERSAND = r'&'
t_CARET = r'\^'
t_PIPE = r'\|'
t_EQ = r'=='
t_NEQ = r'!='
t_GT = r'>'
t_LT = r'<'
t_GEQ = r'>='
t_LEQ = r'<='
t_LPAREN = r'\('
t_RPAREN = r'\)'
t_LBRACKET = r'\['
t_RBRACKET = r'\]'
t_LBRACE = r'\{'
t_RBRACE = r'\}'
t_COMMA = r','
t_DOT = r'\.'
t_NOT = r'!'
t_TILDE = r'~'
t_ignore = ' \t'

def t_IDENTIFIER(t):
    r'[a-zA-Z][a-zA-Z0-9_]*'
    t.type = 'IDENTIFIER'
    return t

def t_newline(t):
    r'\n+'
    t.lexer.lineno += len(t.value)

def t_error(t):
    print(f"Illegal character '{t.value[0]}'")
    t.lexer.skip(1)

lexer = lex.lex()

# Define precedence and associativity
precedence = (
    ('left', 'OR'),
    ('left', 'AND'),
    ('left', 'EQ', 'NEQ', 'GT', 'LT', 'GEQ', 'LEQ'),
    ('left', 'PLUS', 'MINUS'),
    ('left', 'TIMES', 'DIVIDE'),
    ('right', 'UMINUS'),
)

# Grammar rules
def p_expr_name(p):
    'expr : IDENTIFIER'
    p[0] = p[1]

def p_expr_literal(p):
    'expr : STRING_LITERAL'
    p[0] = p[1]

def p_expr_binop(p):
    '''expr : expr PLUS expr
            | expr MINUS expr
            | expr TIMES expr
            | expr DIVIDE expr
            | expr AMPERSAND expr
            | expr CARET expr
            | expr PIPE expr
            | expr EQ expr
            | expr NEQ expr
            | expr GT expr
            | expr LT expr
            | expr GEQ expr
            | expr LEQ expr'''
    p[0] = (p[2], p[1], p[3])

def p_expr_unop(p):
    '''expr : NOT expr
            | TILDE expr
            | MINUS expr %prec UMINUS'''
    p[0] = (p[1], p[2])

def p_expr_parentheses(p):
    'expr : LPAREN expr RPAREN'
    p[0] = p[2]

def p_expr_function_expr(p):
    'expr : DOT IDENTIFIER LPAREN args RPAREN'
    p[0] = ('function_call', p[2], p[4])

def p_args(p):
    'args : expr'
    p[0] = [p[1]]

def p_args_multiple(p):
    'args : args COMMA expr'
    p[0] = p[1] + p[3]

def p_error(p):
    print(f"Syntax error at line {p.lineno}, position {p.lexpos}: Unexpected token '{p.value}'")

parser = yacc.yacc()

# Test the parser
input_expr = 'x + 42'
result = parser.parse(input_expr)
print("Parsed result:", result)
