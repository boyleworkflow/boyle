from dataclasses import dataclass
from typing import Collection, Generic, Hashable, Mapping, NewType, Protocol, TypeVar


Op = TypeVar("Op", bound=Hashable)

Loc = NewType("Loc", str)
Result = NewType("Result", str)
SandboxKey = NewType("SandboxKey", str)


@dataclass
class Calc(Generic[Op]):
    inp: Mapping[Loc, Result]
    op: Op
    out: Collection[Loc]


class Env(Protocol[Op]):
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


def run(calc: Calc[Op], env: Env[Op]) -> Mapping[Loc, Result]:
    sandbox = env.create_sandbox()
    try:
        for loc, digest in calc.inp.items():
            env.place(sandbox, loc, digest)
        env.run_op(calc.op, sandbox)
        results = {loc: env.stow(sandbox, loc) for loc in calc.out}
        return results
    finally:
        env.destroy_sandbox(sandbox)
