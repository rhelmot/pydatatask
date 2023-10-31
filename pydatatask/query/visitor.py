from abc import ABC, abstractmethod

from .parser import (
    BoolLiteral,
    Expression,
    FuncExpr,
    FunctionCall,
    IdentExpression,
    IntLiteral,
    KeyLiteral,
    ListLiteral,
    StringLiteral,
)


class Visitor(ABC):
    def visit(self, obj):
        name = type(obj).__name__
        func = getattr(self, f"visit_{name}")
        return func(obj)

    def visit_Expression(self, obj: Expression):
        raise TypeError(f"Unknown expression type {type(obj)}")

    @abstractmethod
    def visit_IdentExpression(self, obj: IdentExpression):
        raise NotImplementedError

    @abstractmethod
    def visit_StringLiteral(self, obj: StringLiteral):
        raise NotImplementedError

    @abstractmethod
    def visit_KeyLiteral(self, obj: KeyLiteral):
        raise NotImplementedError

    @abstractmethod
    def visit_IntLiteral(self, obj: IntLiteral):
        raise NotImplementedError

    @abstractmethod
    def visit_BoolLiteral(self, obj: BoolLiteral):
        raise NotImplementedError

    @abstractmethod
    def visit_ListLiteral(self, obj: ListLiteral):
        raise NotImplementedError

    @abstractmethod
    def visit_FuncExpr(self, obj: FuncExpr):
        raise NotImplementedError

    @abstractmethod
    def visit_FunctionCall(self, obj: FunctionCall):
        raise NotImplementedError
