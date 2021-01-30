from dataclasses import dataclass
from typing import Iterator, Mapping, Sequence
import pytest
from unittest.mock import Mock
from boyleworkflow.graph import Node, PathLike
from boyleworkflow.scheduling import (
    GraphState,
    get_nodes_and_ancestors,
    get_root_nodes,
)

ROOT_NODE = Node.create({}, "op", "out")
DERIVED_NODE = Node.create({"inp": ROOT_NODE}, "op", "out")


NetworkSpec = Mapping[str, Sequence[str]]


def build_node_network(parents_by_name: NetworkSpec) -> Mapping[str, Node]:
    nodes = {}
    for name, parent_names in parents_by_name.items():
        parents: Mapping[PathLike, Node] = {n: nodes[n] for n in parent_names}
        nodes[name] = Node.create(parents, f"op_{name}", f"out_{name}")
    return nodes


@dataclass
class RequestAndStatesSpec:
    spec: NetworkSpec
    requested_names: Sequence[str]
    number_of_states: int

    @property
    def requested_nodes(self):
        nodes = build_node_network(self.spec)
        return [node for name, node in nodes.items() if name in self.requested_names]


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


def get_failed_invariants(state: GraphState):
    invariant_checks = [
        InvariantCheck(
            "all_nodes == requested and their ancestors",
            state.all_nodes == get_nodes_and_ancestors(state.requested),
        ),
        InvariantCheck(
            "nodes are marked known if and only if they have results",
            state.known == frozenset(state.results.keys()),
        ),
        InvariantCheck(
            "a node cannot be restorable without being known",
            state.restorable <= state.known,
        ),
        InvariantCheck(
            "A node may not be known without its parents being known",
            state.known <= state.parents_known,
        ),
        InvariantCheck(
            "Node.parents is in sync with known and parents_known",
            state.parents_known
            == frozenset(n for n in state.all_nodes if n.parents <= state.known),
        ),
        InvariantCheck(
            "runnable is nonempty (at the very least root nodes can be run)",
            len(state.runnable) > 0,
        ),
        InvariantCheck(
            "Node.parents is in sync with runnable and restorable",
            state.runnable
            == frozenset(n for n in state.all_nodes if n.parents <= state.restorable),
        ),
        InvariantCheck(
            "priority_work is empty if and only if requested <= restorable",
            (not state.priority_work) == (state.requested <= state.restorable),
        ),
    ]
    failed = [c.description for c in invariant_checks if not c.result]
    return failed


def test_init_state():
    requested = [ROOT_NODE]
    root_nodes = frozenset(get_root_nodes(*requested))
    state = GraphState.from_requested(requested)
    assert not state.known
    assert state.parents_known == root_nodes
    assert state.runnable == root_nodes
    assert not state.restorable
    assert state.priority_work == root_nodes


def test_can_add_results():
    state = GraphState.from_requested([ROOT_NODE])
    results = {ROOT_NODE: Mock()}
    updated = state.add_results(results)
    assert updated.results == results


def test_can_add_same_results_twice():
    state = GraphState.from_requested([ROOT_NODE])
    results = {ROOT_NODE: Mock()}
    updated = state.add_results(results).add_results(results)
    assert updated.results == results


def test_cannot_add_conflicting_results():
    state = GraphState.from_requested([ROOT_NODE])
    results1 = {ROOT_NODE: Mock()}
    results2 = {ROOT_NODE: Mock()}
    updated = state.add_results(results1)
    with pytest.raises(ValueError):
        updated.add_results(results2)


def test_only_allow_add_result_if_parents_known():
    state = GraphState.from_requested([DERIVED_NODE])
    assert DERIVED_NODE not in state.parents_known
    results = {DERIVED_NODE: Mock()}
    with pytest.raises(ValueError):
        state.add_results(results)


def test_can_add_restorable():
    state = GraphState.from_requested([ROOT_NODE])
    results = {ROOT_NODE: Mock()}
    updated = state.add_results(results).add_restorable({ROOT_NODE})
    assert set(updated.restorable) == {ROOT_NODE}


def test_only_add_restorable_if_known():
    state = GraphState.from_requested([ROOT_NODE])
    with pytest.raises(ValueError):
        state.add_restorable({ROOT_NODE})


def test_invariants_on_init():
    state = GraphState.from_requested([ROOT_NODE])
    assert not get_failed_invariants(state)


def test_invariants_along_simple_modifications():
    state = GraphState.from_requested([DERIVED_NODE])
    assert not get_failed_invariants(state)
    results = {ROOT_NODE: Mock()}
    with_parent_known = state.add_results(results)
    assert not get_failed_invariants(with_parent_known)
    with_parent_restorable = with_parent_known.add_restorable(results)
    assert not get_failed_invariants(with_parent_restorable)


@pytest.mark.parametrize("network_spec", simple_networks)  # type: ignore
def test_priority_work_leads_to_finish(network_spec: RequestAndStatesSpec):
    state = GraphState.from_requested(network_spec.requested_nodes)

    while state.priority_work:
        state = state.add_results({node: Mock() for node in state.priority_work})
        state = state.add_restorable(state.priority_work)

    assert state.requested <= state.restorable


@pytest.mark.parametrize("network_spec", simple_networks)  # type: ignore
def test_invariants_along_permitted_paths(network_spec: RequestAndStatesSpec):
    start_state = GraphState.from_requested(network_spec.requested_nodes)
    count = 0
    for state in generate_allowed_states(start_state):
        count += 1
        assert not get_failed_invariants(state)
    assert network_spec.number_of_states == count
