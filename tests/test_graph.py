from dataclasses import dataclass
from typing import Mapping, Optional, Sequence
import pytest
from pytest import fixture
from unittest.mock import Mock, call
from boyleworkflow.calc import Loc
from boyleworkflow.graph import Digest, GraphState, Node, get_root_nodes


@dataclass(frozen=True)
class MockOp:
    name: Optional[str] = None


@fixture
def root_node():
    return Node({}, MockOp(), Loc("out"))


@fixture
def derived_node(root_node):
    return Node({Loc("a"): root_node}, MockOp(), Loc("out"))


def build_node_network(
    parents_by_name: Mapping[str, Sequence[str]]
) -> Mapping[str, Node]:
    nodes = {}
    for name, parents in parents_by_name.items():
        nodes[name] = Node(
            {Loc(parent_name): nodes[parent_name] for parent_name in parents},
            MockOp(f"{name}_op"),
            Loc(f"{name}_out"),
        )
    return nodes


def test_nodes_correctly_hashable():
    node_network_spec = {
        "root1": [],
        "root2": [],
        "derived": ["root1", "root2"],
    }

    nodes_a = set(build_node_network(node_network_spec).values())
    nodes_b = set(build_node_network(node_network_spec).values())

    assert nodes_a == nodes_b

    ids_a = set(map(id, nodes_a))
    ids_b = set(map(id, nodes_b))

    assert ids_a.isdisjoint(ids_b)


def test_parents():
    nodes = build_node_network(
        {
            "root": [],
            "mid": ["root"],
            "end": ["mid"],
        }
    )
    assert nodes["root"].parents == set()
    assert nodes["mid"].parents == {nodes["root"]}
    assert nodes["end"].parents == {nodes["mid"]}


def test_root():
    nodes = build_node_network(
        {
            "root1": [],
            "root2": [],
            "mid1": ["root1"],
            "mid2": ["root1", "root2"],
            "bottom1": ["mid1"],
            "bottom2": ["mid2"],
        }
    )

    assert get_root_nodes(nodes["root1"]) == {nodes["root1"]}
    assert get_root_nodes(nodes["bottom1"]) == {nodes["root1"]}
    assert get_root_nodes(nodes["bottom2"]) == {nodes["root1"], nodes["root2"]}


def test_root_node_runnable_on_init(root_node):
    state = GraphState.from_requested([root_node])
    assert state.runnable == {root_node}


def test_root_node_parents_known_on_init(root_node):
    state = GraphState.from_requested([root_node])
    assert state.parents_known == {root_node}


def test_root_node_not_known_on_init(root_node):
    state = GraphState.from_requested([root_node])
    assert state.known == set()


def test_root_node_not_restorable_on_init(root_node):
    state = GraphState.from_requested([root_node])
    assert state.restorable == set()


def test_root_node_priority_work_on_init(root_node):
    state = GraphState.from_requested([root_node])
    assert state.priority_work == {root_node}


def test_root_node_is_all_nodes(root_node):
    state = GraphState.from_requested([root_node])
    assert state.all_nodes == {root_node}


def test_no_results_on_init(root_node):
    state = GraphState.from_requested([root_node])
    assert not state.results


def test_can_add_results(root_node):
    state = GraphState.from_requested([root_node])
    results = {root_node: Mock()}
    updated = state.add_results(results)
    assert updated.results == results


def test_known_after_adding_result(root_node):
    state = GraphState.from_requested([root_node])
    results = {root_node: Mock()}
    updated = state.add_results(results)
    assert set(updated.results) == updated.known


def test_can_update_restorable(root_node):
    state = GraphState.from_requested([root_node])
    results = {root_node: Mock()}
    updated = state.add_results(results).add_restorable({root_node})
    assert set(updated.restorable) == {root_node}


def test_restorable_must_have_result(root_node):
    state = GraphState.from_requested([root_node])
    with pytest.raises(ValueError):
        state.add_restorable({root_node})


def test_derived_and_parent_is_all_nodes(root_node, derived_node):
    state = GraphState.from_requested([derived_node])
    assert state.all_nodes == {root_node, derived_node}


def test_derived_parents_known_iff_parent_result_added(root_node, derived_node):
    state = GraphState.from_requested([derived_node])
    assert derived_node not in state.parents_known
    results = {root_node: Mock()}
    with_parent_known = state.add_results(results)
    assert derived_node in with_parent_known.parents_known


def test_runnable_iff_parent_restorable(root_node, derived_node):
    state = GraphState.from_requested([derived_node])
    assert derived_node not in state.runnable
    results = {root_node: Mock()}
    with_parent_known = state.add_results(results)
    assert derived_node not in with_parent_known.runnable
    with_parent_restorable = with_parent_known.add_restorable(results)
    assert derived_node in with_parent_restorable.runnable
