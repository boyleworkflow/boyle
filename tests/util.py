from typing import Mapping, Union
from boyleworkflow.tree import Tree, Name, Leaf

StrTreeItem = Union["StrTree", str]
StrTree = Mapping[str, StrTreeItem]


def _create_tree_item(value: StrTreeItem):
    if isinstance(value, str):
        return Leaf(value)
    else:
        return tree_from_dict(value)


def tree_from_dict(d: StrTree) -> Tree:
    return Tree({Name(k): _create_tree_item(v) for k, v in d.items()})
