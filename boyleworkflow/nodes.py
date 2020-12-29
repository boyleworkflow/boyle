from __future__ import annotations
from dataclasses import dataclass
from boyleworkflow.calc import Loc, Op
from typing import (
    Collection,
    FrozenSet,
    Generic,
    Mapping,
    Set,
    Union,
)


@dataclass(frozen=True)
class NodeBundle(Generic[Op]):
    inp: Mapping[Loc, Node[Op]]
    op: Op
    out: FrozenSet[Loc]

    @property
    def nodes(self: NodeBundle[Op]) -> FrozenSet[Node[Op]]:
        return frozenset({Node(self, loc) for loc in self.out})

    def __hash__(self):
        return hash((tuple(sorted(self.inp.items())), self.op, self.out))


@dataclass(frozen=True)
class Node(Generic[Op]):
    bundle: NodeBundle[Op]
    out: Loc

    @property
    def parents(self) -> FrozenSet[Node[Op]]:
        return frozenset(self.bundle.inp.values())


LocLike = Union[str, Loc]
NodeInpLike = Mapping[LocLike, Node]


def create_simple_node(inp: NodeInpLike, op: Op, out: LocLike) -> Node[Op]:
    out = Loc(out)
    node = NodeBundle({Loc(k): v for k, v in inp.items()}, op, frozenset([out]))
    return Node(node, out)


def create_sibling_nodes(
    inp: NodeInpLike, op: Op, out: Collection[LocLike]
) -> Set[Node[Op]]:
    out_locs = frozenset(map(Loc, out))
    node = NodeBundle({Loc(k): v for k, v in inp.items()}, op, out_locs)
    return {Node(node, loc) for loc in out_locs}
