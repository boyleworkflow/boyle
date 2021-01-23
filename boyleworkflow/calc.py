from __future__ import annotations
from dataclasses import dataclass
from typing import Any, FrozenSet, Mapping, NewType, Protocol
from boyleworkflow.tree import Path, Tree, TreeItem


Op = Any  # TODO replace this with something more specific
SandboxKey = NewType("SandboxKey", str)


@dataclass(frozen=True)
class CalcBundle:
    inp: Tree
    op: Op
    out: FrozenSet[Path]


@dataclass(frozen=True)
class Calc:
    inp: Tree
    op: Op
    out: Path


class Env(Protocol):
    def run_op(self, op: Op, sandbox: SandboxKey):
        ...

    def create_sandbox(self) -> SandboxKey:
        ...

    def destroy_sandbox(self, sandbox: SandboxKey):
        ...

    def place(self, sandbox: SandboxKey, tree: Tree):
        ...

    def stow(self, sandbox: SandboxKey, path: Path) -> TreeItem:
        ...

    def deliver(self, tree: Tree):
        ...


def run(calc_bundle: CalcBundle, env: Env) -> Mapping[Path, TreeItem]:
    sandbox = env.create_sandbox()
    try:
        env.place(sandbox, calc_bundle.inp)
        env.run_op(calc_bundle.op, sandbox)
        return {path: env.stow(sandbox, path) for path in calc_bundle.out}
    finally:
        env.destroy_sandbox(sandbox)
