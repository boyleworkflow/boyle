from dataclasses import dataclass
from typing import Mapping, Optional, Sequence
import pytest
from pytest import fixture
from unittest.mock import Mock, call
from boyleworkflow.calc import Loc
from boyleworkflow.graph import Node, NodeInp, get_root_nodes


@dataclass(frozen=True)
class MockOp:
    name: Optional[str] = None


def build_node_network(
    parents_by_name: Mapping[str, Sequence[str]]
) -> Mapping[str, Node]:
    nodes = {}
    for name, parents in parents_by_name.items():
        nodes[name] = Node(
            NodeInp.from_dict(
                {
                    Loc(parent_name): nodes[parent_name]
                    for parent_name in parents
                }
            ),
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


def test_node_inp_from_empty_dict():
    assert NodeInp(()) == NodeInp.from_dict({})


def test_node_may_not_have_duplicate_input_locs():
    root1 = Node(NodeInp(()), MockOp(), Loc("out1"))
    root2 = Node(NodeInp(()), MockOp(), Loc("out2"))
    with pytest.raises(ValueError):
        inp = NodeInp(((Loc("a"), root1), (Loc("a"), root2)))


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
