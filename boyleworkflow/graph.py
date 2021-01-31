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
from boyleworkflow.tree import Name, Path, Tree
from boyleworkflow.calc import Op

PathLike = Union[Path, str]
NameLike = Union[Name, str]

# Allowing Collection[PathLike] is possible but opens for mistakes because
# str is also a Collection[PathLike]. So 'abc' could be confused with ['a', 'b', 'c']
PathLikePlural = Union[Tuple[PathLike, ...], List[PathLike], AbstractSet[PathLike]]
NameLikePlural = Union[Tuple[NameLike, ...], List[NameLike], AbstractSet[NameLike]]


def _ensure_path(value: PathLike) -> Path:
    if isinstance(value, str):
        return Path.from_string(value)
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
    inp: FrozenDict[Path, Node]

    def __post_init__(self):
        self.inp_levels  # to raise an error if input levels do not match

    @property
    def parents(self) -> FrozenSet[Node]:
        return frozenset(self.inp.values())

    def __getitem__(self, key: PathLike) -> Node:
        return self.pick(key)

    def pick(self, path: PathLike) -> Node:
        path = _ensure_path(path)
        return PickNode(FrozenDict({Path(): self}), path)

    def nest(self, path: PathLike) -> Node:
        path = _ensure_path(path)
        return RenameNode(FrozenDict({path: self}))

    def merge(self, *other: Node) -> Node:
        nodes = (self,) + other
        inp = {Path.from_string(str(i)): node for (i, node) in enumerate(nodes)}
        return MergeNode(FrozenDict(inp))

    def split(self, level: NameLike) -> Node:
        level = _ensure_name(level)
        return SplitNode(FrozenDict({Path(): self}), level)

    @property
    def inp_levels(self) -> Tuple[Name, ...]:
        return _get_out_levels(self.parents)

    @property
    def out_levels(self) -> Tuple[Name, ...]:
        return self.inp_levels

    @property
    def depth(self) -> int:
        return len(self.out_levels)


@dataclass(frozen=True)
class VirtualNode(Node):
    def run(self, input_tree: Tree) -> Tree:
        raise NotImplemented


@dataclass(frozen=True)
class PickNode(VirtualNode):
    pick_path: Path

    def run(self, input_tree: Tree) -> Tree:
        return input_tree.map_level(self.depth, lambda tree: tree.pick(self.pick_path))


@dataclass(frozen=True)
class RenameNode(VirtualNode):
    def run(self, input_tree: Tree) -> Tree:
        return input_tree


@dataclass(frozen=True)
class MergeNode(VirtualNode):
    def run(self, input_tree: Tree) -> Tree:
        return Tree.merge(subtree for _, subtree in input_tree.iter_level(1))


@dataclass(frozen=True)
class SplitNode(VirtualNode):
    level: Name

    def __post_init__(self):
        if self.level in self.inp_levels:
            raise ValueError(
                f"trying to add duplicate level {self.level} to {self.inp_levels}"
            )

    def run(self, input_tree: Tree) -> Tree:
        return input_tree

    @property
    def out_levels(self):
        return self.inp_levels + (self.level,)


@dataclass(frozen=True)
class EnvNode(Node):
    op: Op
    out: FrozenSet[Path]
