from __future__ import annotations
import dataclasses
from dataclasses import dataclass
import itertools
from typing import (
    Any,
    FrozenSet,
    Iterable,
    Mapping,
    Optional,
    Protocol,
    Set,
)
import boyleworkflow.graph
import boyleworkflow.tree

Node = boyleworkflow.graph.Node
Result = boyleworkflow.tree.Tree


def get_nodes_and_ancestors(nodes: Iterable[Node]) -> FrozenSet[Node]:
    seen: Set[Node] = set()
    new = set(nodes)
    while new:
        seen.update(new)
        new = frozenset(itertools.chain(*(node.parents for node in new))) - seen
    return frozenset(seen)


def get_root_nodes(*nodes: Node) -> FrozenSet[Node]:
    return frozenset({n for n in get_nodes_and_ancestors(nodes) if not n.parents})


@dataclass
class GraphState:
    all_nodes: FrozenSet[Node]
    requested: FrozenSet[Node]
    parents_known: FrozenSet[Node]
    known: FrozenSet[Node]
    runnable: FrozenSet[Node]
    restorable: FrozenSet[Node]
    priority_work: FrozenSet[Node]
    results: Mapping[Node, Result]

    @classmethod
    def from_requested(cls, requested: Iterable[Node]) -> GraphState:
        all_nodes = frozenset(get_nodes_and_ancestors(requested))
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

    def _update(self, **changes: Any):
        return dataclasses.replace(self, **changes)

    def _get_priority_work(self):
        # TODO: think carefully about this.
        #
        # First, we are done if and only if requested <= restorable.
        #
        # Otherwise:
        #
        # We want to avoid running nodes if possible.
        # Therefore, in first place we return unknown nodes whose parents are known
        # (i.e., the frontier of the set of known nodes). These can be recalled
        # from cache if possible, and otherwise needs to be run.
        #
        # If there are no unknown with known parents, then all are known.
        #
        # In that case we are either
        # (a) done, if requested <= restorable, or
        # (b) there are some runnable but not restorable to run.
        #
        # In case (b), a simple and reasonable solution is to prioritize working
        # on any requested & runnable, then all others. But probably this can lead
        # to unnecessary runs. How to avoid that?

        if self.requested <= self.restorable:
            return frozenset()

        return (
            self.parents_known - self.known
            or self.runnable & self.requested - self.restorable
            or self.runnable - self.restorable  # more optimal choice here?
        )

    def _set_priority_work(self):
        return self._update(priority_work=self._get_priority_work())

    def _check_results_not_added_before_parents(self, results: Mapping[Node, Result]):
        added_before_parents = set(results) - self.parents_known
        if added_before_parents:
            raise ValueError(
                "cannot accept results for the following nodes "
                f"because their parents are not known: {added_before_parents}"
            )

    def _check_results_not_conflicting(self, results: Mapping[Node, Result]):
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

    def add_results(self, results: Mapping[Node, Result]):
        self._check_results_not_added_before_parents(results)
        self._check_results_not_conflicting(results)

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


class NodeRunner(Protocol):
    def run(self, node: Node, results: Mapping[Node, Result]) -> Result:
        ...

    def recall(self, node: Node, results: Mapping[Node, Result]) -> Optional[Result]:
        ...

    def can_restore(self, result: Result) -> bool:
        ...


def _advance_state(state: GraphState, system: NodeRunner) -> GraphState:
    nodes = state.priority_work

    new_results = {}
    new_restorable = set()

    for node in nodes:
        result = system.recall(node, state.results) or system.run(node, state.results)
        new_results[node] = result
        if system.can_restore(result):
            new_restorable.add(node)

    return state.add_results(new_results).add_restorable(new_restorable)


def make(requested: Iterable[Node], system: NodeRunner) -> Mapping[Node, Result]:
    requested = set(requested)
    state = GraphState.from_requested(requested)
    while state.priority_work:
        state = _advance_state(state, system)
    return {node: state.results[node] for node in requested}
