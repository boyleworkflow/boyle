from __future__ import annotations
from dataclasses import dataclass
from typing import Any, FrozenSet, Hashable, Mapping, NewType, Protocol
from boyleworkflow.tree import Path, Tree


Op = Any  # TODO replace this with something more specific
SandboxKey = NewType("SandboxKey", str)


@dataclass(frozen=True)
class NoOp:
    pass


NO_OP = NoOp()


@dataclass(frozen=True)
class Calc:
    inp: Tree
    op: Op
    out: FrozenSet[Path]


@dataclass(frozen=True)
class CalcOut:
    inp: Tree
    op: Op
    out: Path


class Env(Hashable, Protocol):
    def run_op(self, op: Op, sandbox: SandboxKey):
        ...

    def create_sandbox(self) -> SandboxKey:
        ...

    def destroy_sandbox(self, sandbox: SandboxKey):
        ...

    def place(self, sandbox: SandboxKey, tree: Tree):
        ...

    def stow(self, sandbox: SandboxKey, path: Path) -> Tree:
        ...

    def deliver(self, tree: Tree):
        ...


def run(calc: Calc, env: Env) -> Mapping[Path, Tree]:
    if calc.op is NO_OP:
        return {path: calc.inp.pick(path) for path in calc.out}
    sandbox = env.create_sandbox()
    try:
        env.place(sandbox, calc.inp)
        env.run_op(calc.op, sandbox)
        return {path: env.stow(sandbox, path) for path in calc.out}
    finally:
        env.destroy_sandbox(sandbox)
