from __future__ import annotations
from boyleworkflow.calc import Calc, Env, run
from boyleworkflow.graph import Node, EnvNode, VirtualNode
import dataclasses
from dataclasses import dataclass
import itertools
from typing import (
    Any,
    FrozenSet,
    Iterable,
    Iterator,
    Mapping,
    Set,
    Union,
)
from boyleworkflow.tree import Path, Tree


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
    results: Mapping[Node, Tree]

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

    def _check_results_not_added_before_parents(self, results: Mapping[Node, Tree]):
        added_before_parents = set(results) - self.parents_known
        if added_before_parents:
            raise ValueError(
                "cannot accept results for the following nodes "
                f"because their parents are not known: {added_before_parents}"
            )

    def _check_results_not_conflicting(self, results: Mapping[Node, Tree]):
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

    def add_results(self, results: Mapping[Node, Tree]):
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


def _run_virtual_node(node: VirtualNode, input_tree: Tree) -> Tree:
    return node.run(input_tree)


def _build_calcs(node: EnvNode, input_tree: Tree) -> Mapping[Path, Calc]:
    return {
        index: Calc(calc_inp, node.op, node.out)
        for index, calc_inp in input_tree.iter_level(node.depth)
    }


def _run_env_node(node: EnvNode, input_tree: Tree, env: Env) -> Tree:
    calcs = _build_calcs(node, input_tree)
    calc_results = {
        index: Tree.from_nested_items(run(calc, env)) for index, calc in calcs.items()
    }
    node_result = Tree.from_nested_items(calc_results)
    return node_result


def _run_node(node: Node, results: Mapping[Node, Tree], env: Env) -> Tree:
    input_tree = Tree.merge(
        results[parent].map_level(node.depth, lambda tree: tree.nest(path))
        for path, parent in node.inp.items()
    )
    if isinstance(node, VirtualNode):
        return _run_virtual_node(node, input_tree)
    elif isinstance(node, EnvNode):
        return _run_env_node(node, input_tree, env)
    else:
        raise ValueError(f"unknown node type {type(node)}")


def _run_priority_work(state: GraphState, env: Env) -> Mapping[Node, Tree]:
    nodes = state.priority_work
    return {node: _run_node(node, state.results, env) for node in nodes}


def _advance_state(state: GraphState, env: Env) -> GraphState:
    results = _run_priority_work(state, env)
    return state.add_results(results).add_restorable(results)


def make(requested: Union[Node, Iterable[Node]], env: Env):
    if isinstance(requested, Node):
        requested = {requested}
    else:
        requested = set(requested)
    state = GraphState.from_requested(requested)
    while state.priority_work:
        state = _advance_state(state, env)
    result_tree = Tree.merge(state.results[node] for node in requested)
    env.deliver(result_tree)
