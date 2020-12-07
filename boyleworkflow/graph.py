import itertools
from boyleworkflow.calc import Loc, Op
import dataclasses
from dataclasses import dataclass
from typing import (
    Any,
    Collection,
    FrozenSet,
    Iterable,
    Mapping,
    NewType,
    Set,
    Tuple,
)


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
class InvariantCheck:
    description: str
    result: bool


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

    def failed_invariants(self):
        invariant_checks = [
            InvariantCheck(
                "all_nodes == requested and its ancestors",
                self.all_nodes
                == frozenset(_iter_nodes_and_ancestors(self.requested)),
            ),
            InvariantCheck(
                "nodes are marked known if and only if they have results",
                self.known == frozenset(self.results.keys()),
            ),
            InvariantCheck(
                "a node cannot be restorable without being known",
                self.restorable <= self.known,
            ),
            InvariantCheck(
                "A node may not be known without its parents being known",
                # (technically it is conceivable, but it does not make sense
                # and likely would be a mistake if it happened)
                self.known <= self.parents_known,
            ),
            InvariantCheck(
                "Node.parents is in sync with known and parents_known",
                self.parents_known
                == frozenset(
                    n for n in self.all_nodes if n.parents <= self.known
                ),
            ),
            InvariantCheck(
                "runnable is nonempty (at the very least root nodes can be run)",
                len(self.runnable) > 0,
            ),
            InvariantCheck(
                "Node.parents is in sync with runnable and restorable",
                self.runnable
                == frozenset(
                    n for n in self.all_nodes if n.parents <= self.restorable
                ),
            ),
            InvariantCheck(
                "priority_work is empty if and only if requested <= restorable",
                (not self.priority_work) == (self.requested <= self.restorable),
            ),
        ]
        failed = [c.description for c in invariant_checks if not c.result]
        return failed

    @classmethod
    def from_requested(cls, requested: Iterable[Node]) -> "GraphState":
        all_nodes = frozenset(_iter_nodes_and_ancestors(requested))
        requested = frozenset(requested)
        root_nodes = get_root_nodes(*requested)
        return GraphState(
            all_nodes=all_nodes,
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

    def _set_priority_work(self):
        # TODO: think carefully about this. In first place we want to run
        # nodes that are unknown. But if that is not possible, we need to run
        # some known but not restorable node. This strategy is simple and
        # should probably guarantee that it will finish. But are there
        # more efficient ways to backtrace than to run everything runnable
        # that is not restorable?
        not_known = self.all_nodes - self.known
        priority_work = self.runnable & not_known
        if not priority_work:
            not_restorable = self.all_nodes - self.restorable
            priority_work = self.runnable & not_restorable
        return self._update(priority_work=priority_work)

    def add_results(self, results: Mapping[Node, Digest]):
        updated_results = {**self.results, **results}
        updated_known = self.known.union(results)
        updated_parents_known = frozenset(
            node for node in self.all_nodes if node.parents <= updated_known
        )
        return self._update(
            results=updated_results,
            known=updated_known,
            parents_known=updated_parents_known,
        )._set_priority_work()

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
        )._set_priority_work()
