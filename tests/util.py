from typing import Any, List, Mapping, Union
from boyleworkflow.tree import Tree, Name, Loc
from boyleworkflow.graph import Node, EnvNode
from boyleworkflow.frozendict import FrozenDict

StrTreeItem = Union["StrTree", str]
StrTree = Mapping[str, StrTreeItem]


def _create_tree_item(value: StrTreeItem):
    if isinstance(value, str):
        return Tree({}, value)
    else:
        return tree_from_dict(value)


def tree_from_dict(d: StrTree) -> Tree:
    return Tree({Name(k): _create_tree_item(v) for k, v in d.items()})


def create_env_node(inp: Mapping[str, Node], op: Any, out: List[str]):
    return EnvNode(
        FrozenDict({Loc.from_string(loc): node for loc, node in inp.items()}),
        op,
        frozenset(map(Loc.from_string, out)),
    )
