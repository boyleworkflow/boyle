from typing import List, Mapping, Union
from boyleworkflow.loc import Name, Loc
from boyleworkflow.tree import Tree
from boyleworkflow.graph import Node, EnvNode
from boyleworkflow.frozendict import FrozenDict
from boyleworkflow.util import JSONData, freeze

StrTreeItem = Union["StrTree", str]
StrTree = Mapping[str, StrTreeItem]


def _create_tree_item(value: StrTreeItem):
    if isinstance(value, str):
        return Tree({}, value)
    else:
        return tree_from_dict(value)


def tree_from_dict(d: StrTree) -> Tree:
    return Tree({Name(k): _create_tree_item(v) for k, v in d.items()})


def create_env_node(inp: Mapping[str, Node], op: JSONData, out: List[Union[str, Loc]]):
    return EnvNode(
        FrozenDict({Loc(loc): node for loc, node in inp.items()}),
        freeze(op),
        frozenset(v if isinstance(v, Loc) else Loc(v) for v in out),
    )
