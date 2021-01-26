from typing import Mapping, Union, Collection, Set
from boyleworkflow.tree import Tree, Name, Path
from boyleworkflow.calc import Op
from boyleworkflow.nodes import Node, NodeBundle

StrTreeItem = Union["StrTree", str]
StrTree = Mapping[str, StrTreeItem]


def _create_tree_item(value: StrTreeItem):
    if isinstance(value, str):
        return Tree({}, value)
    else:
        return tree_from_dict(value)


def tree_from_dict(d: StrTree) -> Tree:
    return Tree({Name(k): _create_tree_item(v) for k, v in d.items()})

