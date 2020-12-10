from boyleworkflow.calc import Loc, Op
from dataclasses import dataclass
from typing import (
    Iterable,
    Mapping,
    Optional,
    Set,
)


@dataclass(frozen=True)
class Node:
    inp: Mapping[Loc, "Node"]
    op: Op
    out: Loc
    name: Optional[str] = None

    @property
    def parents(self) -> Set["Node"]:
        return set(self.inp.values())

    def __hash__(self):
        return hash((tuple(sorted(self.inp.items())), self.op, self.out))

    def __repr__(self):
        if self.name:
            return f"<Node {self.name}>"
        return super().__repr__()


def iter_nodes_and_ancestors(nodes: Iterable[Node]) -> Iterable[Node]:
    seen: Set[Node] = set()
    new = set(nodes)
    while True:
        if not new:
            return
        yield from new
        new = set.union(*(node.parents for node in new)) - seen
        seen.update(new)


def get_root_nodes(*nodes: Node) -> Set[Node]:
    return {n for n in iter_nodes_and_ancestors(nodes) if not n.inp}
