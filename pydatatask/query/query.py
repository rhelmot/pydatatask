from typing import Any, Dict

import yaml

from pydatatask import repository as repomodule
from pydatatask.utils import AWriteStreamBase

from .builtins import builtins, checked_incast
from .executor import Executor, FunctionDefinition, QueryValue, Scope
from .parser import Expression, FunctionType, QueryValueType, expr_parser


class Query:
    def __init__(
        self,
        result_type: QueryValueType,
        query: str,
        parameters: Dict[str, QueryValueType],
        getters: Dict[str, QueryValueType],
        repos: Dict[str, "repomodule.Repository"],
    ):
        self.result_type = result_type
        self.expr: Expression = expr_parser.parse(query)
        self.parameters = parameters
        self.getters = getters
        self.repos = repos

    def checked_incast(self, ty: QueryValueType, val: Any, reason: str) -> QueryValue:
        if ty == QueryValueType.String and isinstance(val, int):
            val = str(val)
        return checked_incast(ty, val, reason)

    def make_getter(self, getter: str, ty: QueryValueType) -> FunctionDefinition:
        async def inner(scope: Scope):
            gotten = scope.lookup_value("arg").unwrap()
            if not isinstance(gotten, dict):
                raise TypeError(f"Can only run get{getter} on dicts, got {gotten}")
            return checked_incast(ty, gotten[getter], f"Return value of get{getter}")

        return FunctionDefinition([], ["arg"], FunctionType((), (QueryValueType.RepositoryData,), ty), inner)

    def make_scope(self, parameters: Dict[str, Any]) -> Scope:
        values = {}
        functions = dict(builtins)
        if set(parameters) != set(self.parameters):
            raise ValueError(f"Passed incorrect parameters: got {set(parameters)}, expected {set(self.parameters)}")
        for getter, ty in self.getters.items():
            functions[f"get{getter}"] = [self.make_getter(getter, ty)]
        for name, repo in self.repos.items():
            values[name] = QueryValue.wrap(repo)
        for name, ty in self.parameters.items():
            values[name] = self.checked_incast(ty, parameters[name], f"Parameter {name} to Query")
        return Scope.base_scope(values, functions)

    async def execute(self, parameters: Dict[str, Any]) -> QueryValue:
        scope = self.make_scope(parameters)
        executor = Executor(scope)
        return await executor.visit(self.expr)

    async def format_response(self, result: QueryValue, response: AWriteStreamBase):
        if self.result_type in (QueryValueType.String, QueryValueType.Key) and result.type == QueryValueType.String:
            str_result = result.string_value
            assert str_result is not None
        elif (
            self.result_type in (QueryValueType.String, QueryValueType.Key)
            and result.type == QueryValueType.RepositoryData
            and isinstance(result.data_value, str)
        ):
            str_result = result.data_value
        else:
            str_result = yaml.safe_dump(result.unwrap())
        await response.write(str_result.encode())
