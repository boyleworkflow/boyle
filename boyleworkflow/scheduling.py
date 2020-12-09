import dataclasses
from dataclasses import dataclass
from typing import (
    FrozenSet,
    Iterable,
    Iterator,
    Mapping,
    NewType,
)
from boyleworkflow.nodes import Node, _iter_nodes_and_ancestors, get_root_nodes


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

    def get_failed_invariants(self):
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

    def _generate_priority_suggestions(self) -> Iterator[FrozenSet[Node]]:
        # TODO: think carefully about this. In first place we want to run
        # nodes that are unknown. But if that is not possible, we need to run
        # some known but not restorable node. This strategy is simple and
        # should probably guarantee that it will finish. But are there
        # more efficient ways to backtrace than to run everything runnable
        # that is not restorable?

        # If any node is unknown, then we cannot be done.
        # So in first place, run runnable nodes that are unknown.
        not_known = self.all_nodes - self.known
        yield self.runnable & not_known

        # Now there is a chance that all requested are restorable.
        # If so, we are done.
        not_restorable = self.all_nodes - self.restorable
        requested_nonrestorable = self.requested & not_restorable
        if not requested_nonrestorable:
            return

        # If not, those are prioritized
        yield requested_nonrestorable & self.runnable

        # Then take the rest
        # TODO: Create a more optimal choice here
        yield not_restorable & self.runnable

    def _get_priority_work(self):
        for suggestion in self._generate_priority_suggestions():
            if suggestion:
                return suggestion
        return frozenset()

    def _set_priority_work(self):
        return self._update(priority_work=self._get_priority_work())

    def add_results(self, results: Mapping[Node, Digest]):
        added_before_parents = set(results) - self.parents_known
        if added_before_parents:
            raise ValueError(
                "cannot accept results for the following nodes "
                f"because their parents are not known: {added_before_parents}"
            )
        conflicting_nodes = {
            node: (self.results[node], new_result)
            for node, new_result in results.items()
            if node in self.results and new_result != self.results[node]
        }
        if conflicting_nodes:
            raise ValueError(
                "cannot add conflicting results "
                f"for the following nodes: {conflicting_nodes}"
            )
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