from __future__ import annotations
from boyleworkflow.frozendict import FrozenDict
from boyleworkflow.util import JSONData, freeze
from functools import wraps
from typing import AbstractSet, Any, Callable, List, Mapping, Tuple, Union, overload
from boyleworkflow.loc import Loc, Name
from boyleworkflow.nodes import CalcNode, Node, PickNode, PutNode, SplitNode
from dataclasses import dataclass


LocLike = Union[Loc, str]
NameLike = Union[Name, str]

# Allowing Collection[LocLike] opens for mistakes because
# str is also a Collection[LocLike]. So 'abc' could be confused with ['a', 'b', 'c']
LocLikePlural = Union[Tuple[LocLike, ...], List[LocLike], AbstractSet[LocLike]]
NameLikePlural = Union[Tuple[NameLike, ...], List[NameLike], AbstractSet[NameLike]]


def _ensure_loc(value: LocLike) -> Loc:
    if isinstance(value, str):
        return Loc(value)
    else:
        return value


def _ensure_name(value: NameLike) -> Name:
    if isinstance(value, str):
        return Name(value)
    else:
        return value


def wrap_node_output(func: Callable[..., Node]) -> Callable[..., NodeWrapper]:
    @wraps(func)
    def decorated(*args: Any, **kwargs: Any):
        node = func(*args, **kwargs)
        return NodeWrapper(node)

    return decorated


@dataclass(frozen=True)
class NodeWrapper:
    node: Node

    def __post_init__(self):
        if isinstance(self.node, CalcNode) and len(self.node.out) > 1:
            raise ValueError(
                f"trying to create NodeWrapper with multi-output node: {self.node.out}"
            )

    @wrap_node_output
    def split(self, level: NameLike):
        return SplitNode.from_node(self.node, _ensure_name(level))

    @wrap_node_output
    def pick(self, loc: LocLike):
        return PickNode.from_node(self.node, _ensure_loc(loc))

    def __getitem__(self, key: LocLike):
        return self.pick(key)


@dataclass(frozen=True)
class MultiWrapper:
    node: CalcNode

    @wrap_node_output
    def pick(self, loc: LocLike):
        loc = _ensure_loc(loc)
        if loc not in self.node.out:
            raise KeyError(f"no key '{loc}' among {self.node.out}")
        return PickNode.from_node(self.node, _ensure_loc(loc))

    def __getitem__(self, key: LocLike):
        return self.pick(key)


NodeOrWrapper = Union[Node, NodeWrapper, MultiWrapper]


def _unwrap(v: NodeOrWrapper) -> Node:
    return v if isinstance(v, Node) else v.node


def _build_inp(inp: Mapping[LocLike, NodeOrWrapper]):
    return FrozenDict(
        {_ensure_loc(loc): _unwrap(parent) for loc, parent in inp.items()}
    )


@wrap_node_output
def put(inp: Mapping[LocLike, NodeOrWrapper]):
    return PutNode(_build_inp(inp))


@overload
def define(
    inp: Mapping[LocLike, NodeOrWrapper], op: JSONData, out: LocLike
) -> NodeWrapper:
    ...


@overload
def define(
    inp: Mapping[LocLike, NodeOrWrapper], op: JSONData, out: LocLikePlural
) -> MultiWrapper:
    ...


def define(
    inp: Mapping[LocLike, NodeOrWrapper],
    op: JSONData,
    out: Union[LocLike, LocLikePlural],
):
    if isinstance(out, (Loc, str)):
        one_out = True
        out_conv = frozenset({_ensure_loc(out)})
    else:
        one_out = False
        out_conv = frozenset(map(_ensure_loc, out))

    node = CalcNode(
        _build_inp(inp),
        freeze(op),
        out_conv,
    )

    if one_out:
        (the_loc,) = out_conv
        return NodeWrapper(node).pick(the_loc)
    else:
        return MultiWrapper(node)
