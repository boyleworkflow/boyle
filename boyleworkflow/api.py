from typing import Iterable, Union, Optional, Tuple, cast, Any
from enum import Enum

import attr

from boyleworkflow.core import Op, Comp, Loc
from boyleworkflow.ops import SpecialFilePath, ShellOp, RenameOp
from boyleworkflow.storage import Storage, Digest


@attr.s(auto_attribs=True)
class File:
    loc: str


Resource = Union[File, SpecialFilePath, Loc, str]


def _get_loc(resource: Resource) -> Loc:
    if isinstance(resource, str):
        return Loc(resource)

    if isinstance(resource, SpecialFilePath):
        return resource.value

    return Loc(resource.loc)


TaskInputs = Union[Comp, Iterable[Comp]]
def ensure_comp_tuple(value: TaskInputs) -> Tuple[Comp, ...]:
    print(value)
    try:
        values = cast(Iterable[Comp], value)
        return tuple(values)
    except TypeError:
        the_value = cast(Comp, value)
        return (the_value,)


@attr.s(auto_attribs=True, frozen=True)
class Task:
    op: Op
    inputs: Iterable[Comp] = attr.ib(converter=ensure_comp_tuple, default=())

    @inputs.validator
    def validate(instance, attribute, inputs):
        forbidden = [SpecialFilePath.STDOUT.value, SpecialFilePath.STDERR.value]
        for inp in inputs:
            if inp.loc in forbidden:
                raise ValueError(
                    f"loc '{inp.loc}' not allowed as input (try renaming it)"
                )

    def out(self, resource: Resource):
        forbidden = [SpecialFilePath.STDIN.value]
        loc = _get_loc(resource)
        if loc in forbidden:
            raise ValueError(
                f"loc '{loc}' not allowed as output (try renaming it)"
            )

        return Comp(self.op, self.inputs, loc)

    @property
    def stdout(self):
        if not self.op.stdout:
            raise ValueError("stdout not activated on this task")
        return self.out(SpecialFilePath.STDOUT)

    @property
    def stderr(self):
        if not self.op.stderr:
            raise ValueError("stderr not activated on this task")
        return self.out(SpecialFilePath.STDERR)


def rename(comp: Comp, new_loc: Loc) -> Comp:
    op = RenameOp(comp.loc, new_loc)
    return Comp(op, [comp], new_loc)


def shell(
    cmd, inputs: TaskInputs = (), stdin: Optional[Comp] = None, **kwargs
):
    inputs = ensure_comp_tuple(inputs)
    for inp in inputs:
        if isinstance(inp, Task):
            raise ValueError("Cannot use task as input. Forgot task.out()?")

    if stdin is not None:
        inputs = inputs + (rename(stdin, SpecialFilePath.STDIN.value),)
    op = ShellOp(cmd, stdin=(stdin is not None), shell=True, **kwargs)
    return Task(op, inputs)


def load_value(digest: Digest, storage: Storage) -> Any:
    raise NotImplementedError


def python(func: Callable, *args, **kwargs):
    raise NotImplementedError
