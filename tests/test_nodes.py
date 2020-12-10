from boyleworkflow.calc import Loc
from dataclasses import dataclass
from typing import Mapping

from pytest import fixture
from tests.node_helpers import build_node_network, NetworkSpec
from boyleworkflow.nodes import Node, get_root_nodes, iter_nodes_and_ancestors


@fixture
def simple_node_network_spec() -> NetworkSpec:
    return {
        "root1": [],
        "root2": [],
        "derived": ["root1", "root2"],
    }


@fixture
def nodes_a(simple_node_network_spec):
    return build_node_network(simple_node_network_spec)


@fixture
def nodes_b(simple_node_network_spec):
    return build_node_network(simple_node_network_spec)


def test_node_networks_are_separate_objects(nodes_a, nodes_b):
    # sort of a meta-test to check that the test data are what we expect
    assert len(nodes_a) == 3
    assert len(nodes_a) == len(nodes_b)
    ids_a = set(map(id, nodes_a.values()))
    ids_b = set(map(id, nodes_b.values()))
    assert ids_a.isdisjoint(ids_b)


def test_different_nodes_hash_unequal(nodes_a):
    hashes = set(map(hash, nodes_a.values()))
    assert len(hashes) == 3


def test_equality_and_hash_insensitive_to_inp_order():
    root1 = Node({}, "op1", Loc("out1"))
    root2 = Node({}, "op2", Loc("out2"))
    derived_a = Node({Loc("i1"): root1, Loc("i2"): root2}, "op", Loc("out"))
    derived_b = Node({Loc("i2"): root2, Loc("i1"): root1}, "op", Loc("out"))
    assert derived_a == derived_b
    assert hash(derived_a) == hash(derived_b)


def test_identical_nodes_hash_equal(nodes_a, nodes_b):
    hashes_a = set(map(hash, nodes_a.values()))
    hashes_b = set(map(hash, nodes_b.values()))
    assert hashes_a == hashes_b


def test_identical_nodes_compare_equal(nodes_a, nodes_b):
    assert nodes_a == nodes_b


def test_iter_nodes_and_ancestors():
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

    set(iter_nodes_and_ancestors([nodes["root1"]])) == {nodes["root1"]}
    set(iter_nodes_and_ancestors([nodes["bottom2"]])) == {
        nodes["root1"],
        nodes["root2"],
        nodes["mid2"],
        nodes["bottom2"],
    }


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
