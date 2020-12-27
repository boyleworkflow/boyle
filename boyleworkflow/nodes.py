from dataclasses import dataclass
from boyleworkflow.calc import Loc, Op
from typing import (
    FrozenSet,
    Mapping,
    Optional,
)

@dataclass(frozen=True)
class Node:
    inp: Mapping[Loc, "Node"]
    op: Op
    out: Loc
    name: Optional[str] = None

    @property
    def parents(self) -> FrozenSet["Node"]:
        return frozenset(self.inp.values())

    def __hash__(self):
        return hash((tuple(sorted(self.inp.items())), self.op, self.out))

    def __repr__(self):
        if self.name:
            return f"<Node {self.name}>"
        return super().__repr__()
