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


def _iter_nodes_and_ancestors(nodes: Iterable[Node]) -> Iterable[Node]:
    seen: Set[Node] = set()
    new = set(nodes)
    while True:
        if not new:
            return
        yield from new
        new = set.union(*(node.parents for node in new)) - seen
        seen.update(new)


def get_root_nodes(*nodes: Node) -> Set[Node]:
    return {n for n in _iter_nodes_and_ancestors(nodes) if not n.inp}
