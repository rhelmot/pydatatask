from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum, auto
from itertools import product

from typing_extensions import Mapping, TypeAlias

from pydatatask.query.visitor import Visitor
from pydatatask.repository import Repository


class QueryValueType(Enum):
    Bool = auto()
    Int = auto()
    String = auto()
    Key = auto()
    List = auto()
    Repository = auto()
    RepositoryData = auto()


@dataclass
class QueryValue:
    type: QueryValueType
    bool_value: Optional[bool] = None
    int_value: Optional[int] = None
    string_value: Optional[str] = None
    key_value: Optional[str] = None
    list_value: Optional[List["QueryValue"]] = None
    repo_value: Optional[Repository] = None
    data_value: Optional[Any] = None

    def typecheck(self, ty: "TemplateType"):
        return isinstance(ty, QueryValueType) and self.type == ty


TemplateType: TypeAlias = Union["FunctionType", QueryValueType]
ArgTypes: TypeAlias = Tuple[QueryValueType, ...]
TemplateTypes: TypeAlias = Tuple[TemplateType, ...]
TemplateValue: TypeAlias = Union[QueryValue, "TemplatedFunction"]


@dataclass(frozen=True)
class FunctionType:
    template_types: TemplateTypes
    arg_types: ArgTypes
    return_type: QueryValueType


@dataclass
class FunctionDefinition:
    template_names: List[str]
    arg_names: List[str]
    type: FunctionType
    impl: Callable[["Scope"], QueryValue]


@dataclass
class TemplatedFunction:
    template_params: List[TemplateValue]
    defns: Dict[ArgTypes, FunctionDefinition]
    name: str

    def make_scope(self, base_scope: "Scope", args: Iterable[QueryValue]) -> Tuple["Scope", FunctionDefinition]:
        args = list(args)
        argtypes = tuple(arg.type for arg in args)

        if argtypes not in self.defns:
            raise NameError(f"No overload available for {self.name} that matches {argtypes}")
        defn = self.defns[argtypes]

        local_functions: Dict[str, TemplatedFunction] = {}
        local_values = dict(zip(defn.arg_names, args))
        for name, value, ty in zip(defn.template_names, self.template_params, defn.type.template_types):
            if isinstance(ty, QueryValueType):
                assert isinstance(value, QueryValue)
                local_values[name] = value
            else:
                assert isinstance(ty, FunctionType)
                assert isinstance(value, TemplatedFunction)
                local_functions[name] = value

        return (
            Scope(
                local_values,
                base_scope.global_values,
                base_scope.global_functions,
                local_functions,
                base_scope.global_templates,
            ),
            defn,
        )


@dataclass
class ResolvedFunction:
    defn: FunctionDefinition
    template_params: List[TemplateValue]


@dataclass
class Scope:
    local_values: Dict[str, QueryValue]
    global_values: Dict[str, QueryValue]
    global_functions: Dict[str, Dict[ArgTypes, FunctionDefinition]]
    local_functions: Dict[str, TemplatedFunction]
    global_templates: Dict[Tuple[str, TemplateTypes], Dict[ArgTypes, FunctionDefinition]]

    @classmethod
    def base_scope(cls, values: Mapping[str, QueryValue], functions: Mapping[str, List[FunctionDefinition]]) -> "Scope":
        global_functions: Mapping[str, Dict[ArgTypes, FunctionDefinition]] = defaultdict(dict)
        global_templates: Mapping[Tuple[str, TemplateTypes], Dict[ArgTypes, FunctionDefinition]] = defaultdict(dict)
        for name, funclist in functions.items():
            for func in funclist:
                if func.type.template_types:
                    global_templates[(name, func.type.template_types)][func.type.arg_types] = func
                else:
                    global_functions[name][func.type.arg_types] = func

        return Scope({}, dict(values), global_functions, {}, global_templates)

    def lookup_value(self, name: str) -> QueryValue:
        r = self.local_values.get(name, None)
        if r is not None:
            return r
        return self.global_values[name]

    def lookup_function(self, name: str, args: Iterable[Union[QueryValue, QueryValueType]]) -> ResolvedFunction:
        argtypes = tuple(arg if isinstance(arg, QueryValueType) else arg.type for arg in args)
        if name in self.local_functions and argtypes in self.local_functions[name].defns:
            return ResolvedFunction(
                self.local_functions[name].defns[argtypes], self.local_functions[name].template_params
            )
        if name in self.global_functions and argtypes in self.global_functions[name]:
            return ResolvedFunction(self.global_functions[name][argtypes], [])
        if name in self.local_functions or name in self.global_functions:
            raise NameError(f"No overload available for {name} that matches {argtypes}")
        if any(availname == name for availname, _ in self.global_templates):
            raise NameError(f"Function {name} exists, but needs to be templated first")
        raise NameError(f"No function with name {name}")

    def lookup_template(
        self, name: str, args: Iterable[Union[TemplateValue, TemplateType]]
    ) -> Dict[ArgTypes, FunctionDefinition]:
        args = list(args)
        if name in self.local_functions or name in self.global_functions:
            if args:
                raise NameError(f"Function {name} exists, but has already been templated")
            if name in self.local_functions:
                return self.local_functions[name].defns
            return self.global_functions[name]

        result: Dict[ArgTypes, FunctionDefinition] = {}
        targtype_options = [
            [arg]
            if isinstance(arg, (QueryValueType, FunctionType))
            else arg.defns
            if isinstance(arg, TemplatedFunction)
            else [arg.type]
            for arg in args
        ]
        for targtypes in product(*targtype_options):
            if (name, targtypes) in self.global_templates:
                result.update(self.global_templates[(name, targtypes)])
        if result:
            return result
        if any(availname == name for availname, _ in self.global_templates):
            raise NameError(f"No overload available for {name} that matches the given template arguments")
        raise NameError(f"No function with name {name}")


class Executor(Visitor):
    def __init__(self, scope: Scope):
        self.scope = scope

    def visit_IdentExpression(self, obj) -> QueryValue:
        return self.scope.lookup_value(obj.name)

    def visit_IntLiteral(self, obj) -> QueryValue:
        return QueryValue(QueryValueType.Int, int_value=obj.value)

    def visit_StringLiteral(self, obj) -> QueryValue:
        return QueryValue(QueryValueType.String, string_value=obj.value)

    def visit_BoolLiteral(self, obj) -> QueryValue:
        return QueryValue(QueryValueType.Bool, bool_value=obj.value)

    def visit_KeyLiteral(self, obj) -> QueryValue:
        return QueryValue(QueryValueType.Key, key_value=obj.value)

    def visit_ListLiteral(self, obj) -> QueryValue:
        return QueryValue(QueryValueType.List, list_value=[self.visit(expr) for expr in obj.values])

    def visit_FuncExpr(self, obj) -> TemplatedFunction:
        targs = [self.visit(expr) for expr in obj.template_args]
        defns = self.scope.lookup_template(obj.name, targs)
        return TemplatedFunction(targs, defns, obj.name)

    def visit_FunctionCall(self, obj) -> QueryValue:
        funcexpr = self.visit_FuncExpr(obj.function)
        args = [self.visit(expr) for expr in obj.args]
        scope, defn = funcexpr.make_scope(self.scope, args)
        return defn.impl(scope)
