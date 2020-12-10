from boyleworkflow.calc import Loc
from dataclasses import dataclass
from typing import Mapping, Optional, Sequence
from pytest import fixture
from boyleworkflow.nodes import Node


@dataclass(frozen=True)
class MockOp:
    name: Optional[str] = None


@fixture
def root_node():
    return Node({}, MockOp(), Loc("out"))


@fixture
def derived_node(root_node):
    return Node({Loc("a"): root_node}, MockOp(), Loc("out"))


NetworkSpec = Mapping[str, Sequence[str]]


def build_node_network(parents_by_name: NetworkSpec) -> Mapping[str, Node]:
    nodes = {}
    for name, parents in parents_by_name.items():
        nodes[name] = Node(
            {Loc(parent_name): nodes[parent_name] for parent_name in parents},
            MockOp(f"{name}_op"),
            Loc(f"{name}_out"),
            name=name,
        )
    return nodes
