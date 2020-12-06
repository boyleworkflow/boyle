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
    """
    Invariants:

    # all_nodes is the minimal graph below requested
    all_nodes == frozenset(_iter_nodes_and_ancestors(requested))

    # nodes are marked known if and only if they have results
    known == frozenset(results.keys())

    #  A node may not be known without its parents being known
    # (technically it is conceivable, but it does not make sense
    # and likely would be a mistake if it happened)
    parents_known <= known

    # the Node.parents property is in sync with known and parents_known
    parents_known == frozenset(n for n in all_nodes if n.parents <= known)

    # runnable is nonempty (at the very least root nodes can be run)
    len(runnable) > 0
    
    # The Node.parents property is in sync with runnable and restorable
    runnable == frozenset(n for n in all_nodes if n.parents <= restorable)

    # priority_work is first to make all known; then make requested restorable.
    if known < all_nodes:
        priority_work == runnable & (all_nodes - known)
    else:
        priority_work == runnable & (all_nodes - restorable)

    # priority_work is empty if and only if requested <= restorable
    (not priority_work) == (requested <= restorable)
    """
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
