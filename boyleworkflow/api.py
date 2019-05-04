from typing import Iterable, Union
from enum import Enum

import attr

from boyleworkflow.core import Op, Comp, Loc
from boyleworkflow.runcalc import SpecialFilePath

@attr.s(auto_attribs=True)
class File:
    loc: str

Resource = Union[File, SpecialFilePath, Loc, str]

def _get_loc(resource: Resource) -> Loc:
    if isinstance(resource, (str, Loc)):
        return Loc(resource)

    if isinstance(resource, SpecialFilePath):
        return resource.value

    return resource.loc


@attr.s(auto_attribs=True)
class Task:
    op: Op
    inputs: Iterable[Comp] = attr.ib(converter=tuple, default=())


    def out(self, resource: Resource):
        return Comp(self.op, self.inputs, _get_loc(resource))


    @property
    def stdout(self):
        if not self.op.stdout:
            raise ValueError('stdout not activated on this task')
        return self.output(SpecialFilePath.STDOUT)


    @property
    def stderr(self):
        if not self.op.stderr:
            raise ValueError('stderr not activated on this task')
        return self.output(SpecialFilePath.STDERR)



def shell(cmd, inputs: Iterable[Comp]=(), **kwargs):
    stdin = SpecialFilePath.STDIN.value in (inp.loc for inp in inputs)
    op = Op(cmd, stdin=stdin, shell=True, **kwargs)
    return Task(op, inputs)
