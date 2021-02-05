from __future__ import annotations
from boyleworkflow.frozendict import FrozenDict
from dataclasses import dataclass
from typing import (
    AbstractSet,
    Collection,
    FrozenSet,
    List,
    Tuple,
    Union,
)
from boyleworkflow.loc import Name, Loc
from boyleworkflow.tree import Tree
from boyleworkflow.calc import Op

LocLike = Union[Loc, str]
NameLike = Union[Name, str]

# Allowing Collection[LocLike] is possible but opens for mistakes because
# str is also a Collection[LocLike]. So 'abc' could be confused with ['a', 'b', 'c']
LocLikePlural = Union[Tuple[LocLike, ...], List[LocLike], AbstractSet[LocLike]]
NameLikePlural = Union[Tuple[NameLike, ...], List[NameLike], AbstractSet[NameLike]]


def _ensure_loc(value: LocLike) -> Loc:
    if isinstance(value, str):
        return Loc.from_string(value)
    else:
        return value


def _ensure_name(value: NameLike) -> Name:
    if isinstance(value, str):
        return Name(value)
    else:
        return value


def _get_out_levels(nodes: Collection[Node]) -> Tuple[Name, ...]:
    nodes = set(nodes)
    if not nodes:
        return ()

    different_levels = {node.out_levels for node in nodes}
    if not len(different_levels) == 1:
        raise ValueError(f"input levels do not match: {different_levels}")

    (levels,) = different_levels
    return levels


@dataclass(frozen=True)
class Node:
    inp: FrozenDict[Loc, Node]

    def __post_init__(self):
        self.inp_levels  # to raise an error if input levels do not match

    @property
    def parents(self) -> FrozenSet[Node]:
        return frozenset(self.inp.values())

    def __getitem__(self, key: LocLike) -> Node:
        return self.pick(key)

    def pick(self, loc: LocLike) -> Node:
        loc = _ensure_loc(loc)
        return PickNode(FrozenDict({Loc(): self}), loc)

    def nest(self, loc: LocLike) -> Node:
        loc = _ensure_loc(loc)
        return RenameNode(FrozenDict({loc: self}))

    def merge(self, *other: Node) -> Node:
        nodes = (self,) + other
        inp = {Loc.from_string(str(i)): node for (i, node) in enumerate(nodes)}
        return MergeNode(FrozenDict(inp))

    def split(self, level: NameLike) -> Node:
        level = _ensure_name(level)
        return SplitNode(FrozenDict({Loc(): self}), level)

    @property
    def inp_levels(self) -> Tuple[Name, ...]:
        return _get_out_levels(self.parents)

    @property
    def out_levels(self) -> Tuple[Name, ...]:
        return self.inp_levels

    @property
    def run_depth(self) -> int:
        return len(self.out_levels)


@dataclass(frozen=True)
class VirtualNode(Node):
    def run_subtree(self, input_tree: Tree) -> Tree:
        raise NotImplemented


@dataclass(frozen=True)
class PickNode(VirtualNode):
    pick_loc: Loc

    def run_subtree(self, input_tree: Tree) -> Tree:
        return input_tree.pick(self.pick_loc)


@dataclass(frozen=True)
class NoOpNode(VirtualNode):
    """
    Base class for VirtualNode that does not alter the tree
    """

    @property
    def run_depth(self):
        return 0

    def run_subtree(self, input_tree: Tree) -> Tree:
        return input_tree


@dataclass(frozen=True)
class RenameNode(NoOpNode):
    pass


@dataclass(frozen=True)
class MergeNode(VirtualNode):
    def run_subtree(self, input_tree: Tree) -> Tree:
        return Tree.merge(subtree for _, subtree in input_tree.iter_level(1))


@dataclass(frozen=True)
class SplitNode(NoOpNode):
    level: Name

    def __post_init__(self):
        if self.level in self.inp_levels:
            raise ValueError(
                f"trying to add duplicate level {self.level} to {self.inp_levels}"
            )

    @property
    def out_levels(self):
        return self.inp_levels + (self.level,)


@dataclass(frozen=True)
class EnvNode(Node):
    op: Op
    out: FrozenSet[Loc]
