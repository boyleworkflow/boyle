__version__ = "0.2.0"

from dataclasses import dataclass
from typing import Any, Generic, Iterable, Mapping, Protocol, TypeVar


class Op(Protocol):
    ...


Sandbox = TypeVar("Sandbox")

CalcInput = Any


@dataclass
class Calc:
    inp: Iterable[CalcInput]
    op: Op


class Env(Generic[Sandbox], Protocol):
    def run_op(self, op: Op, sandbox: Sandbox):
        ...

    def create_sandbox(self) -> Sandbox:
        ...

    def destroy_sandbox(self, sandbox: Sandbox):
        ...

    def place(self, item: CalcInput, sandbox: Sandbox):
        ...


def run(calc: Calc, env: Env):
    sandbox = env.create_sandbox()
    try:
        for item in calc.inp:
            env.place(item, sandbox)
        env.run_op(calc.op, sandbox)
    finally:
        env.destroy_sandbox(sandbox)
