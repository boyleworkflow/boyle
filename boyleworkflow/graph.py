import itertools
from boyleworkflow.calc import Loc, Op
from dataclasses import dataclass
from typing import Collection, Iterable, Mapping, Set, Tuple

@dataclass(frozen=True)
class NodeInp:
    pairs: Tuple[Tuple[Loc, "Node"], ...]

    @classmethod
    def from_dict(cls, d: Mapping[Loc, "Node"]):
        return cls(tuple(d.items()))

    def __post_init__(self):
        inp_locs = [pair[0] for pair in self.pairs]
        if len(set(inp_locs)) < len(inp_locs):
            raise ValueError(f"duplicate locs: {inp_locs}")

    
@dataclass(frozen=True)
class Node:
    inp: NodeInp
    op: Op
    out: Loc

    @property
    def parents(self) -> Set["Node"]:
        return set(pair[1] for pair in self.inp.pairs)
