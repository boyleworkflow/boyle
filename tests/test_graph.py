from dataclasses import dataclass
from typing import Mapping, Optional, Sequence
import pytest
from pytest import fixture
from unittest.mock import Mock
from boyleworkflow.calc import Loc
from boyleworkflow.graph import GraphState, Node, get_root_nodes


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


def test_node_parents():
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


def test_get_root_nodes():
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
    assert not state.failed_invariants()


def test_invariants_along_modifications(root_node, derived_node):
    state = GraphState.from_requested([derived_node])
    assert not state.failed_invariants()
    results = {root_node: Mock()}
    with_parent_known = state.add_results(results)
    assert not with_parent_known.failed_invariants()
    with_parent_restorable = with_parent_known.add_restorable(results)
    assert not with_parent_restorable.failed_invariants()
