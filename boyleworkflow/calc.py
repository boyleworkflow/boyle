from dataclasses import dataclass
from typing import Any, Collection, Mapping, NewType, Protocol


Op = Any  # TODO replace this with something more specific
Loc = NewType("Loc", str)
Result = NewType("Result", str)
SandboxKey = NewType("SandboxKey", str)


@dataclass
class Calc:
    inp: Mapping[Loc, Result]
    op: Op
    out: Collection[Loc]


class Env(Protocol):
    def run_op(self, op: Op, sandbox: SandboxKey):
        ...

    def create_sandbox(self) -> SandboxKey:
        ...

    def destroy_sandbox(self, sandbox: SandboxKey):
        ...

    def place(self, sandbox: SandboxKey, loc: Loc, digest: Result):
        ...

    def stow(self, sandbox: SandboxKey, loc: Loc) -> Result:
        ...

    def deliver(self, loc: Loc, digest: Result):
        ...


def run(calc: Calc, env: Env) -> Mapping[Loc, Result]:
    sandbox = env.create_sandbox()
    try:
        for loc, digest in calc.inp.items():
            env.place(sandbox, loc, digest)
        env.run_op(calc.op, sandbox)
        results = {loc: env.stow(sandbox, loc) for loc in calc.out}
        return results
    finally:
        env.destroy_sandbox(sandbox)
