from typing import (
    DefaultDict,
    List,
    Optional,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)
from collections import defaultdict
from collections.abc import Callable as ABCCallable
import inspect

from pydatatask.query.executor import (
    FunctionDefinition,
    FunctionType,
    QueryValue,
    QueryValueType,
    Scope,
    TemplatedFunction,
)
from pydatatask.repository.base import Repository

builtins: DefaultDict[str, List[FunctionDefinition]] = defaultdict(list)


class Key(str):
    pass


def checked_outcast(ty: Union[QueryValueType, FunctionType], value: Union[QueryValue, TemplatedFunction], scope: Scope):
    if isinstance(value, TemplatedFunction):
        assert isinstance(ty, FunctionType)
        assert not ty.template_types

        def inner(*args):
            assert isinstance(ty, FunctionType)
            if len(args) != len(ty.arg_types):
                raise TypeError(f"Bad number of args: Expected {len(ty.arg_types)}, got {len(args)}")
            inargs = [checked_incast(argty, arg) for arg, argty in zip(args, ty.arg_types)]
            (
                newscope,
                defn,
            ) = value.make_scope(scope, inargs)
            return checked_outcast(ty.return_type, defn.impl(newscope), scope)

        return inner
    assert ty == value.type
    if ty == QueryValueType.String:
        return value.string_value
    if ty == QueryValueType.Key:
        return Key(value.key_value)
    if ty == QueryValueType.Int:
        return value.int_value
    if ty == QueryValueType.Bool:
        return value.bool_value
    if ty == QueryValueType.Repository:
        return value.repo_value
    if ty == QueryValueType.List:
        return value.list_value
    return value.data_value


def checked_incast_template(ty: Union[QueryValueType, FunctionType], value) -> Union[FunctionDefinition, QueryValue]:
    if isinstance(ty, FunctionType):
        assert callable(ty)
        return wrap_function(value)
    return checked_incast(ty, value)


def checked_incast(ty: QueryValueType, value) -> QueryValue:
    if ty == QueryValueType.String:
        assert isinstance(value, str)
        return QueryValue(ty, string_value=value)
    if ty == QueryValueType.Key:
        assert isinstance(value, Key)
        return QueryValue(ty, key_value=str(value))
    if ty == QueryValueType.Int:
        assert isinstance(value, int)
        return QueryValue(ty, int_value=value)
    if ty == QueryValueType.Bool:
        assert isinstance(value, bool)
        return QueryValue(ty, bool_value=value)
    if ty == QueryValueType.Repository:
        assert isinstance(value, Repository)
        return QueryValue(ty, repo_value=value)
    return QueryValue(ty, data_value=value)


def translate_pytype_template(ty) -> Union[QueryValueType, FunctionType]:
    if get_origin(ty) is ABCCallable:
        args, retval = get_args(ty)
        return FunctionType((), tuple(translate_pytype(a) for a in args), translate_pytype(retval))
    return translate_pytype(ty)


def translate_pytype(ty) -> QueryValueType:
    if ty is int:
        return QueryValueType.Int
    if ty is str:
        return QueryValueType.String
    if ty is bool:
        return QueryValueType.Bool
    if issubclass(ty, Repository):
        return QueryValueType.Repository
    if get_origin(ty) is list:
        return QueryValueType.List
    if ty is Key:
        return QueryValueType.Key
    return QueryValueType.RepositoryData


def wrap_function(f, template_args: Optional[List[str]] = None) -> FunctionDefinition:
    pytypes = get_type_hints(f)
    spec = inspect.getfullargspec(f)
    all_args = spec.args
    args = list(all_args)
    template_nargs = template_args or []
    for argname in template_nargs:
        args.remove(argname)
    template_types = tuple(translate_pytype_template(pytypes[name]) for name in template_nargs)
    arg_types = tuple(translate_pytype(pytypes[name]) for name in args)
    ty = FunctionType(template_types, arg_types, translate_pytype(pytypes["return"]))

    def inner(scope: Scope):
        stuff = [
            checked_outcast(
                arg_types[args.index(a)] if a in args else template_types[template_nargs.index(a)],
                scope.lookup_value(a),
                scope,
            )
            for a in all_args
        ]
        return checked_incast(ty.return_type, f(*stuff))

    obj = FunctionDefinition(template_nargs, args, ty, inner)
    return obj


def builtin(name: Optional[str] = None, template_args: Optional[List[str]] = None):
    def wrapper(f):
        obj = wrap_function(f, template_args)
        builtins[name or f.__name__].append(obj)
        return obj

    return wrapper


@builtin("__add__")
def int_add(a: int, b: int) -> int:
    return a + b


@builtin("__add__")
def str_add(a: str, b: str) -> str:
    return a + b


@builtin("int")
def str_to_int(a: str) -> int:
    return int(a)


@builtin("str")
def int_to_str(a: int) -> str:
    return str(a)

@builtin("__sub__")
def int_sub(a: int, b: int) -> int:
    return a - b


@builtin("__mul__")
def int_mul(a: int, b: int) -> int:
    return a * b

@builtin("__mul__")
def str_mul(a: str, b: int) -> str:
    return a * b


@builtin("__div__")
def int_div(a: int, b: int) -> int:
    return int(a / b)


@builtin("__mod__")
def int_mod(a: int, b: int) -> int:
    return a % b

@builtin("__bitand__")
def int_bitand(a: int, b: int) -> int:
    return a & b

@builtin("__bitand__")
def bool_bitand(a: bool, b: bool) -> bool:
    return a & b

@builtin("__bitor__")
def int_bitor(a: int, b: int) -> int:
    return a | b

@builtin("__bitor__")
def bool_bitor(a: bool, b: bool) -> bool:
    return a | b

@builtin("__bitxor__")
def int_bitxor(a: int, b: int) -> int:
    return a ^ b

@builtin("__bitxor__")
def bool_bitxor(a: bool, b: bool) -> bool:
    return a ^ b

@builtin("__gt__")
def int_gt(a: int, b: int) -> bool:
    return a > b

@builtin("__gt__")
def str_gt(a: str, b: str) -> bool:
    return a > b

@builtin("__gt__")
def bool_gt(a: bool, b: bool) -> bool:
    return a > b

@builtin("__lt__")
def int_lt(a: int, b: int) -> bool:
    return a < b

@builtin("__lt__")
def str_lt(a: str, b: str) -> bool:
    return a < b

@builtin("__lt__")
def bool_lt(a: bool, b: bool) -> bool:
    return a < b

@builtin("__ge__")
def int_ge(a: int, b: int) -> bool:
    return a >= b

@builtin("__ge__")
def str_ge(a: str, b: str) -> bool:
    return a >= b

@builtin("__ge__")
def bool_ge(a: bool, b: bool) -> bool:
    return a >= b

@builtin("__le__")
def int_le(a: int, b: int) -> bool:
    return a <= b

@builtin("__le__")
def str_le(a: str, b: str) -> bool:
    return a <= b

@builtin("__le__")
def bool_le(a: bool, b: bool) -> bool:
    return a <= b

@builtin("__inv__")
def bool_le(a: int) -> int:
    return ~a

@builtin("__not__")
def bool_le(a: bool) -> bool:
    return not a

@builtin("__neg__")
def bool_le(a: int) -> int:
    return -a

@builtin("__plus__")
def bool_le(a: int) -> int:
    return +a