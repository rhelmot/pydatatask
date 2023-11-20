from typing import (
    Any,
    Awaitable,
    Callable,
    DefaultDict,
    List,
    Optional,
    TypeAlias,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)
from collections import defaultdict
from collections.abc import Awaitable as ABCAwaitable
from collections.abc import Callable as ABCCallable
import inspect

from pydatatask.query.executor import (
    FunctionDefinition,
    FunctionType,
    QueryValue,
    QueryValueType,
    ResolvedFunction,
    Scope,
)
from pydatatask.repository.base import (
    FilterMetadataRepository,
    FilterRepository,
    FunctionCallMetadataRepository,
    MetadataRepository,
    RelatedItemMetadataRepository,
    RelatedItemRepository,
    Repository,
)

builtins: DefaultDict[str, List[FunctionDefinition]] = defaultdict(list)


class Key(str):
    pass


IntoQueryValue: TypeAlias = Any


def checked_outcast(ty: Union[QueryValueType, FunctionType], value: Union[QueryValue, ResolvedFunction], scope: Scope):
    if isinstance(value, ResolvedFunction):
        assert isinstance(ty, FunctionType)
        assert not ty.template_types

        async def inner(*args):
            assert isinstance(ty, FunctionType)
            if len(args) != len(ty.arg_types):
                raise TypeError(f"Bad number of args: Expected {len(ty.arg_types)}, got {len(args)}")
            inargs = [checked_incast(argty, arg) for arg, argty in zip(args, ty.arg_types)]
            (
                newscope,
                defn,
            ) = value.make_scope(scope, inargs)
            return checked_outcast(ty.return_type, await defn.impl(newscope), scope)

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


def incast(value: IntoQueryValue) -> QueryValue:
    if isinstance(value, Key):
        return QueryValue(QueryValueType.Key, key_value=str(value))
    if isinstance(value, str):
        return QueryValue(QueryValueType.String, string_value=value)
    if isinstance(value, bool):
        return QueryValue(QueryValueType.Bool, bool_value=value)
    if isinstance(value, int):
        return QueryValue(QueryValueType.Int, int_value=value)
    if isinstance(value, Repository):
        return QueryValue(QueryValueType.Repository, repo_value=value)
    if isinstance(value, list):
        return QueryValue(QueryValueType.List, list_value=value)
    return QueryValue(QueryValueType.RepositoryData, data_value=value)


def checked_incast(ty: QueryValueType, value: IntoQueryValue) -> QueryValue:
    if ty == QueryValueType.RepositoryData:
        return QueryValue(ty, data_value=value)
    result = incast(value)
    assert result.type == ty
    return result


def translate_pytype_template(ty) -> Union[QueryValueType, FunctionType]:
    if get_origin(ty) is ABCCallable:
        args, retval = get_args(ty)
        if get_origin(retval) is not ABCAwaitable:
            raise TypeError("Can only handle async functions")
        (retval,) = get_args(retval)
        return FunctionType((), tuple(translate_pytype(a) for a in args), translate_pytype(retval))
    return translate_pytype(ty)


def translate_pytype(ty) -> QueryValueType:
    if ty is int:
        return QueryValueType.Int
    if ty is str:
        return QueryValueType.String
    if ty is bool:
        return QueryValueType.Bool
    if isinstance(ty, type) and issubclass(ty, Repository):
        return QueryValueType.Repository
    if get_origin(ty) is list:
        return QueryValueType.List
    if ty is Key:
        return QueryValueType.Key
    return QueryValueType.RepositoryData


def wrap_function(f: Callable[..., Awaitable[Any]], template_args: Optional[List[str]] = None) -> FunctionDefinition:
    pytypes = get_type_hints(f)
    spec = inspect.getfullargspec(f)
    all_args = spec.args
    args = list(all_args)
    template_nargs = template_args or []
    for argname in args:
        if argname not in template_nargs and get_origin(pytypes[argname]) is ABCCallable:
            template_nargs.append(argname)
    for argname in template_nargs:
        args.remove(argname)
    template_types = tuple(translate_pytype_template(pytypes[name]) for name in template_nargs)
    arg_types = tuple(translate_pytype(pytypes[name]) for name in args)
    ty = FunctionType(template_types, arg_types, translate_pytype(pytypes["return"]))

    async def inner(scope: Scope):
        all_args_types = [
            arg_types[args.index(a)] if a in args else template_types[template_nargs.index(a)] for a in all_args
        ]
        stuff = [
            checked_outcast(
                ty,
                scope.lookup_value(a) if isinstance(ty, QueryValueType) else scope.lookup_function(a, ty.arg_types),
                scope,
            )
            for ty, a in zip(all_args_types, all_args)
        ]
        return checked_incast(ty.return_type, await f(*stuff))

    obj = FunctionDefinition(template_nargs, args, ty, inner)
    return obj


def builtin(name: Optional[str] = None, template_args: Optional[List[str]] = None):
    def wrapper(f: Callable[..., Awaitable[Any]]):
        obj = wrap_function(f, template_args)
        builtins[name or f.__name__].append(obj)
        return obj

    return wrapper


@builtin("__eq__")
async def int_eq(a: int, b: int) -> bool:
    return a == b


@builtin("__eq__")
async def bool_eq(a: bool, b: bool) -> bool:
    return a == b


@builtin("__eq__")
async def str_eq(a: str, b: str) -> bool:
    return a == b


@builtin("__eq__")
async def key_eq(a: Key, b: Key) -> bool:
    return a == b


@builtin("__add__")
async def int_add(a: int, b: int) -> int:
    return a + b


@builtin("__add__")
async def str_add(a: str, b: str) -> str:
    return a + b


@builtin("int")
async def str_to_int(a: str) -> int:
    return int(a)


@builtin("str")
async def int_to_str(a: int) -> str:
    return str(a)


@builtin("__sub__")
async def int_sub(a: int, b: int) -> int:
    return a - b


@builtin("__mul__")
async def int_mul(a: int, b: int) -> int:
    return a * b


@builtin("__mul__")
async def str_mul(a: str, b: int) -> str:
    return a * b


@builtin("__div__")
async def int_div(a: int, b: int) -> int:
    return int(a / b)


@builtin("__mod__")
async def int_mod(a: int, b: int) -> int:
    return a % b


@builtin("__bitand__")
async def int_bitand(a: int, b: int) -> int:
    return a & b


@builtin("__bitand__")
async def bool_bitand(a: bool, b: bool) -> bool:
    return a & b


@builtin("__bitor__")
async def int_bitor(a: int, b: int) -> int:
    return a | b


@builtin("__bitor__")
async def bool_bitor(a: bool, b: bool) -> bool:
    return a | b


@builtin("__bitxor__")
async def int_bitxor(a: int, b: int) -> int:
    return a ^ b


@builtin("__bitxor__")
async def bool_bitxor(a: bool, b: bool) -> bool:
    return a ^ b


@builtin("__gt__")
async def int_gt(a: int, b: int) -> bool:
    return a > b


@builtin("__gt__")
async def str_gt(a: str, b: str) -> bool:
    return a > b


@builtin("__gt__")
async def bool_gt(a: bool, b: bool) -> bool:
    return a > b


@builtin("__lt__")
async def int_lt(a: int, b: int) -> bool:
    return a < b


@builtin("__lt__")
async def str_lt(a: str, b: str) -> bool:
    return a < b


@builtin("__lt__")
async def bool_lt(a: bool, b: bool) -> bool:
    return a < b


@builtin("__ge__")
async def int_ge(a: int, b: int) -> bool:
    return a >= b


@builtin("__ge__")
async def str_ge(a: str, b: str) -> bool:
    return a >= b


@builtin("__ge__")
async def bool_ge(a: bool, b: bool) -> bool:
    return a >= b


@builtin("__le__")
async def int_le(a: int, b: int) -> bool:
    return a <= b


@builtin("__le__")
async def str_le(a: str, b: str) -> bool:
    return a <= b


@builtin("__le__")
async def bool_le(a: bool, b: bool) -> bool:
    return a <= b


@builtin("__inv__")
def int_inv(a: int) -> int:
    return ~a


@builtin("__not__")
def bool_not(a: bool) -> bool:
    return not a


@builtin("__not__")
def int_not(a: int) -> bool:
    return not bool(a)


@builtin("__not__")
def str_not(a: str) -> bool:
    return not a


@builtin("__neg__")
def int_neg(a: int) -> int:
    return -a


@builtin("__plus__")
def int_plus(a: int) -> int:
    return +a


@builtin("map")
async def map_values(a: Repository, b: Callable[[object], Awaitable[object]]) -> Repository:
    if not isinstance(a, MetadataRepository):
        raise TypeError("Only MetadataRepository can be used with map()")
    return a.map(b)


@builtin("rekey")
async def rekey(a: Repository, b: Callable[[Key], Awaitable[Key]]) -> Repository:
    async def inner(k: str) -> str:
        return str(await b(Key(k)))

    if isinstance(a, MetadataRepository):
        return RelatedItemMetadataRepository(a, FunctionCallMetadataRepository(inner, a))
    else:
        return RelatedItemRepository(a, FunctionCallMetadataRepository(inner, a))


@builtin("filter")
async def filter_repo_keys(a: Repository, b: Callable[[Key], Awaitable[bool]]) -> Repository:
    async def inner(k: str) -> bool:
        return await b(Key(k))

    if isinstance(a, MetadataRepository):
        return FilterMetadataRepository(a, inner)
    else:
        return FilterRepository(a, inner)


@builtin("filter")
async def filter_repo_keyvals(a: Repository, b: Callable[[Key, object], Awaitable[bool]]) -> Repository:
    if not isinstance(a, MetadataRepository):
        raise TypeError("Only MetadataRepository can be used with filter() with key-data callback")

    async def inner(k: str) -> bool:
        v = await a.info(k)
        return await b(Key(k), v)

    async def ident(x):
        return x

    return a.map(ident, inner)


@builtin("any")
async def repo_any(a: Repository) -> bool:
    async for _ in a:
        return True
    else:
        return False


@builtin("__index__")
async def list_index(a: List, b: int) -> object:
    return a[b]


@builtin("__index__")
async def repo_index(a: Repository, b: Key) -> object:
    if not isinstance(a, MetadataRepository):
        raise TypeError("Can only index MetadataRepositories")
    return a.info(str(b))


@builtin("sort")
async def sort_list(a: List) -> List:
    return sorted(a)


@builtin("sort")
async def sort_list_keyed(a: List, key: Callable[[object], Awaitable[int]]) -> List:
    keys = [(await key(x), x) for x in a]
    keys.sort()
    for i, (x, _) in enumerate(keys):
        keys[i] = x  # type: ignore
    return keys


@builtin("values")
async def repo_values(a: Repository) -> List:
    if not isinstance(a, MetadataRepository):
        raise TypeError("Can only values MetadataRepositories")
    return sorted((await a.info_all()).values())


@builtin("keys")
async def repo_keys(a: Repository) -> List:
    return [Key(x) async for x in a]


@builtin("len")
async def repo_len(a: Repository) -> int:
    result = 0
    async for _ in a:
        result += 1
    return result


@builtin("len")
async def list_len(a: List) -> int:
    return len(a)
