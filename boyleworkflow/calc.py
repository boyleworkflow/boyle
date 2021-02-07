from __future__ import annotations
from dataclasses import dataclass
from typing import FrozenSet, NewType, Protocol
from boyleworkflow.tree import Tree
from boyleworkflow.loc import Loc
from boyleworkflow.util import FrozenJSON


SandboxKey = NewType("SandboxKey", str)


Op = FrozenJSON


class Env(Protocol):
    def run_op(self, op: Op, sandbox: SandboxKey):
        ...

    def create_sandbox(self) -> SandboxKey:
        ...

    def destroy_sandbox(self, sandbox: SandboxKey):
        ...

    def can_restore(self, tree: Tree) -> bool:
        ...

    def place(self, sandbox: SandboxKey, tree: Tree):
        ...

    def stow(self, sandbox: SandboxKey, loc: Loc) -> Tree:
        ...

    def deliver(self, tree: Tree):
        ...


Op = FrozenJSON


@dataclass(frozen=True)
class Calc:
    inp: Tree
    op: Op
    out: FrozenSet[Loc]


def run_calc(calc: Calc, env: Env) -> Tree:
    sandbox = env.create_sandbox()
    try:
        env.place(sandbox, calc.inp)
        env.run_op(calc.op, sandbox)
        return Tree.from_nested_items({loc: env.stow(sandbox, loc) for loc in calc.out})
    finally:
        env.destroy_sandbox(sandbox)
