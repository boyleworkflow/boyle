from dataclasses import dataclass
from typing import Any, Collection, NewType, Protocol
from boyleworkflow.tree import Leaf, Path, Tree


Op = Any  # TODO replace this with something more specific
SandboxKey = NewType("SandboxKey", str)


@dataclass
class Calc:
    inp: Tree
    op: Op
    out: Collection[Path]


class Env(Protocol):
    def run_op(self, op: Op, sandbox: SandboxKey):
        ...

    def create_sandbox(self) -> SandboxKey:
        ...

    def destroy_sandbox(self, sandbox: SandboxKey):
        ...

    def place(self, sandbox: SandboxKey, tree: Tree):
        ...

    def stow(self, sandbox: SandboxKey, path: Path) -> Leaf:
        ...

    def deliver(self, tree: Tree):
        ...


def run(calc: Calc, env: Env) -> Tree:
    sandbox = env.create_sandbox()
    try:
        env.place(sandbox, calc.inp)
        env.run_op(calc.op, sandbox)
        results = {path: env.stow(sandbox, path) for path in calc.out}
        return Tree.from_nested_items(results)
    finally:
        env.destroy_sandbox(sandbox)
