from typing import Iterable, Union, Optional, Tuple
from enum import Enum

import attr

from boyleworkflow.core import Op, Comp, Loc
from boyleworkflow.ops import SpecialFilePath, ShellOp, RenameOp


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


def _make_tuple(it: Iterable) -> Tuple:
    return tuple(it)


@attr.s(auto_attribs=True)
class Task:
    op: Op
    inputs: Iterable[Comp] = attr.ib(converter=_make_tuple, default=())

    def out(self, resource: Resource):
        return Comp(self.op, self.inputs, _get_loc(resource))

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
    cmd, inputs: Iterable[Comp] = (), stdin: Optional[Comp] = None, **kwargs
):
    if stdin is not None:
        inputs = tuple(inputs) + (rename(stdin, SpecialFilePath.STDIN.value),)
    op = ShellOp(cmd, stdin=(stdin is not None), shell=True, **kwargs)
    return Task(op, inputs)
