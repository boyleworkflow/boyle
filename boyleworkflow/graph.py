import itertools
from boyleworkflow.calc import Loc, Op
from dataclasses import dataclass
from typing import Collection, Iterable, Mapping, Set, Tuple

@dataclass(frozen=True)
class Node:
    inp: Mapping[Loc, "Node"]
    op: Op
    out: Loc

    @property
    def parents(self) -> Set["Node"]:
        return set(self.inp.values())
    
    def __hash__(self):
        return hash((tuple(self.inp.items()), self.op, self.out))
