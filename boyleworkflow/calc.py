from __future__ import annotations
from dataclasses import dataclass, field
from typing import FrozenSet, Mapping, NewType, Protocol
from boyleworkflow.tree import Path, Tree
from boyleworkflow.util import get_id_str, FrozenJSON


Op = FrozenJSON
SandboxKey = NewType("SandboxKey", str)


@dataclass(frozen=True)
class Calc:
    inp: Tree
    op: Op
    out: FrozenSet[Path]


@dataclass(frozen=True)
class CalcOut:
    calc_out_id: str = field(init=False)
    inp: Tree
    op: Op
    out: Path

    def __post_init__(self):
        object.__setattr__(
            self,
            "calc_out_id",
            get_id_str(
                type(self),
                {
                    "inp": self.inp.tree_id,
                    "op": self.op,
                    "out": self.out.to_string(),
                },
            ),
        )


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

    def stow(self, sandbox: SandboxKey, path: Path) -> Tree:
        ...

    def deliver(self, tree: Tree):
        ...


def run_calc(calc: Calc, env: Env) -> Mapping[Path, Tree]:
    sandbox = env.create_sandbox()
    try:
        env.place(sandbox, calc.inp)
        env.run_op(calc.op, sandbox)
        return {path: env.stow(sandbox, path) for path in calc.out}
    finally:
        env.destroy_sandbox(sandbox)
