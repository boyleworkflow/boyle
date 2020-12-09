import pytest
from unittest.mock import Mock
from boyleworkflow.scheduling import GraphState
from boyleworkflow.nodes import Node, get_root_nodes
from tests.node_helpers import root_node, derived_node

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
    assert not state.get_failed_invariants()


def test_invariants_along_modifications(root_node, derived_node):
    state = GraphState.from_requested([derived_node])
    assert not state.get_failed_invariants()
    results = {root_node: Mock()}
    with_parent_known = state.add_results(results)
    assert not with_parent_known.get_failed_invariants()
    with_parent_restorable = with_parent_known.add_restorable(results)
    assert not with_parent_restorable.get_failed_invariants()
