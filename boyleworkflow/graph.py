import itertools
from boyleworkflow.calc import Loc, Op
import dataclasses
from dataclasses import dataclass
from typing import Collection, FrozenSet, Iterable, Mapping, NewType, Set, Tuple


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


Digest = NewType("Digest", str)


@dataclass
class GraphState:
    all_nodes: FrozenSet[Node]
    requested: FrozenSet[Node]
    parents_known: FrozenSet[Node]
    known: FrozenSet[Node]
    runnable: FrozenSet[Node]
    restorable: FrozenSet[Node]
    priority_work: FrozenSet[Node]
    results: Mapping[Node, Digest]

    @classmethod
    def from_requested(cls, requested: Iterable[Node]) -> "GraphState":
        requested = frozenset(requested)
        root_nodes = get_root_nodes(*requested)
        return GraphState(
            all_nodes=frozenset(_iter_nodes_and_ancestors(requested)),
            requested=requested,
            parents_known=frozenset(root_nodes),
            known=frozenset(),
            runnable=frozenset(root_nodes),
            restorable=frozenset(),
            priority_work=frozenset(root_nodes),
            results={},
        )

    def _update(self, **changes):
        return dataclasses.replace(self, **changes)

    def add_results(self, results: Mapping[Node, Digest]):
        new_results = {**self.results, **results}
        new_known = self.known.union(new_results)
        new_parents_known = frozenset(
            node for node in self.all_nodes if node.parents <= new_known
        )
        return self._update(
            results=new_results,
            known=new_known,
            parents_known=new_parents_known,
        )

    def add_restorable(self, nodes: Iterable[Node]):
        addition = set(nodes)
        added_but_not_known = addition - self.known
        if added_but_not_known:
            raise ValueError(
                "cannot the following set restorable "
                f"because they are not known: {added_but_not_known}"
            )
        new_restorable = self.restorable | addition
        new_runnable = frozenset(
            node for node in self.all_nodes if node.parents <= new_restorable
        )
        return self._update(
            restorable=new_restorable,
            runnable=new_runnable,
        )
