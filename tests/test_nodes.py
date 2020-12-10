from dataclasses import dataclass
from tests.node_helpers import build_node_network
from boyleworkflow.nodes import get_root_nodes, iter_nodes_and_ancestors


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
