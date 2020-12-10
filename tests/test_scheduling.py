from dataclasses import dataclass
from typing import Iterator, Mapping, Sequence
import pytest
from unittest.mock import Mock
from boyleworkflow.scheduling import GraphState
from boyleworkflow.nodes import Node, get_root_nodes, iter_nodes_and_ancestors
from tests.node_helpers import (
    NetworkSpec,
    build_node_network,
    root_node,
    derived_node,
)


@dataclass
class RequestAndStatesSpec:
    spec: NetworkSpec
    requested_names: Sequence[str]
    number_of_states: int

    @property
    def requested_nodes(self):
        nodes = build_node_network(self.spec)
        return [
            node for name, node in nodes.items() if name in self.requested_names
        ]


def _generate_allowed_steps(state: GraphState) -> Iterator[GraphState]:
    allowed_new_result_nodes = state.parents_known - state.known
    for node in allowed_new_result_nodes:
        yield state.add_results({node: Mock()})

    allowed_new_restorable_nodes = state.known - state.restorable
    for node in allowed_new_restorable_nodes:
        yield state.add_restorable({node})


def generate_allowed_states(start_state: GraphState) -> Iterator[GraphState]:
    yield start_state
    for child_state in _generate_allowed_steps(start_state):
        yield from generate_allowed_states(child_state)


simple_networks = [
    RequestAndStatesSpec(
        {
            "A": [],
        },
        ["A"],
        3,
    ),
    RequestAndStatesSpec(
        {
            "A": [],
            "B": ["A"],
            "C": ["B"],
            "D": ["C"],
        },
        ["D"],
        313,
    ),
    RequestAndStatesSpec(
        {
            "A1": [],
            "A2": [],
            "B": ["A1", "A2"],
        },
        ["B"],
        93,
    ),
    RequestAndStatesSpec(
        {
            "A": [],
            "B": ["A"],
            "C1": ["B"],
            "C2": ["B"],
        },
        ["C1", "C2"],
        616,
    ),
]


@dataclass
class InvariantCheck:
    description: str
    result: bool


def get_failed_invariants(self):
    invariant_checks = [
        InvariantCheck(
            "all_nodes == requested and its ancestors",
            self.all_nodes
            == frozenset(iter_nodes_and_ancestors(self.requested)),
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
            == frozenset(n for n in self.all_nodes if n.parents <= self.known),
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


def test_init_state(root_node: Node):
    requested = [root_node]
    root_nodes = frozenset(get_root_nodes(*requested))
    state = GraphState.from_requested(requested)
    assert not state.known
    assert state.parents_known == root_nodes
    assert state.runnable == root_nodes
    assert not state.restorable
    assert state.priority_work == root_nodes


def test_can_add_results(root_node):
    state = GraphState.from_requested([root_node])
    results = {root_node: Mock()}
    updated = state.add_results(results)
    assert updated.results == results


def test_can_add_same_results_twice(root_node):
    state = GraphState.from_requested([root_node])
    results = {root_node: Mock()}
    updated = state.add_results(results).add_results(results)
    assert updated.results == results


def test_cannot_add_conflicting_results(root_node):
    state = GraphState.from_requested([root_node])
    results1 = {root_node: Mock()}
    results2 = {root_node: Mock()}
    updated = state.add_results(results1)
    with pytest.raises(ValueError):
        updated.add_results(results2)


def test_only_allow_add_result_if_parents_known(derived_node):
    state = GraphState.from_requested([derived_node])
    assert derived_node not in state.parents_known
    results = {derived_node: Mock()}
    with pytest.raises(ValueError):
        state.add_results(results)


def test_can_add_restorable(root_node):
    state = GraphState.from_requested([root_node])
    results = {root_node: Mock()}
    updated = state.add_results(results).add_restorable({root_node})
    assert set(updated.restorable) == {root_node}


def test_only_add_restorable_if_known(root_node):
    state = GraphState.from_requested([root_node])
    with pytest.raises(ValueError):
        state.add_restorable({root_node})


def test_invariants_on_init(root_node):
    state = GraphState.from_requested([root_node])
    assert not get_failed_invariants(state)


def test_invariants_along_simple_modifications(root_node, derived_node):
    state = GraphState.from_requested([derived_node])
    assert not get_failed_invariants(state)
    results = {root_node: Mock()}
    with_parent_known = state.add_results(results)
    assert not get_failed_invariants(with_parent_known)
    with_parent_restorable = with_parent_known.add_restorable(results)
    assert not get_failed_invariants(with_parent_restorable)


@pytest.mark.parametrize("network_spec", simple_networks)
def test_priority_work_leads_to_finish(network_spec: RequestAndStatesSpec):
    state = GraphState.from_requested(network_spec.requested_nodes)

    while state.priority_work:
        state = state.add_results(
            {node: Mock() for node in state.priority_work}
        )
        state = state.add_restorable(state.priority_work)

    assert state.requested <= state.restorable


@pytest.mark.parametrize("network_spec", simple_networks)
def test_invariants_along_permitted_paths(network_spec: RequestAndStatesSpec):
    start_state = GraphState.from_requested(network_spec.requested_nodes)
    count = 0
    for state in generate_allowed_states(start_state):
        count += 1
        assert not get_failed_invariants(state)
    assert network_spec.number_of_states == count
