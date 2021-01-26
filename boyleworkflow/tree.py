from __future__ import annotations
from boyleworkflow.frozendict import FrozenDict
from dataclasses import dataclass
from functools import reduce
from typing import Iterable, Mapping, Optional, Tuple, Union

_SEPARATOR = "/"
_DOT = "."
_DOUBLE_DOT = ".."
_FORBIDDEN_NAMES = ["", _DOT, _DOUBLE_DOT]


@dataclass(frozen=True, order=True)
class Name:
    value: str

    def __post_init__(self):
        if self.value in _FORBIDDEN_NAMES:
            raise ValueError(f"invalid Name: {repr(self.value)}")
        if _SEPARATOR in self.value:
            raise ValueError(f"invalid Name with separator: {self.value}")


@dataclass(frozen=True, order=True)
class Path:
    names: Tuple[Name, ...] = ()

    @classmethod
    def from_string(cls, value: str) -> Path:
        if value.endswith(_SEPARATOR):
            value = value[:-1]
        path_segments = (v for v in value.split(_SEPARATOR) if v != _DOT)
        return Path(tuple(map(Name, path_segments)))

    def to_string(self) -> str:
        if self.names:
            return _SEPARATOR.join((n.value for n in self.names))
        else:
            return _DOT

    def __truediv__(self, name: Name) -> Path:
        return Path(self.names + (name,))

    @property
    def parent(self):
        return Path(self.names[:-1])

    def __repr__(self):
        return f"Path('{self.to_string()}')"


class TreeCollision(ValueError):
    pass


TreeData = Optional[str]


@dataclass(frozen=True, init=False)
class Tree:
    children: FrozenDict[Name, Tree]
    data: TreeData

    def __init__(self, children: Mapping[Name, Tree], data: TreeData = None):
        object.__setattr__(self, "children", FrozenDict(children))
        object.__setattr__(self, "data", data)

    def __getitem__(self, key: Name) -> Tree:
        return self.children[key]

    def pick(self, path: Path) -> Tree:
        result = self
        for name in path.names:
            try:
                result = result[name]
            except KeyError:
                raise ValueError(f"no item {name} found")
        return result

    def nest(self, path: Path) -> Tree:
        if not path.names:
            return self
        reversed_names = reversed(path.names)
        tree = Tree({next(reversed_names): self})
        for name in reversed_names:
            tree = Tree({name: tree})
        return tree

    @classmethod
    def from_nested_items(cls, subtrees: Mapping[Path, Tree]) -> Tree:
        empty_tree = Tree({})
        trees = (subtree.nest(path) for path, subtree in subtrees.items())
        return reduce(Tree.merge, trees, empty_tree)

    def _walk_prefixed(self, prefix: Path) -> Iterable[Tuple[Path, Tree]]:
        yield (prefix, self)
        for k, v in self.children.items():
            yield from v._walk_prefixed(prefix / k)

    def walk(self) -> Iterable[Tuple[Path, Tree]]:
        yield from self._walk_prefixed(Path(()))

    def _iter_level(self, level: int, prefix: Path) -> Iterable[Tuple[Path, Tree]]:
        if level == 0:
            yield prefix, self
        else:
            if not self.children:
                raise ValueError("no children found below {prefix}")
            for name, subtree in self.children.items():
                yield from subtree._iter_level(level - 1, (prefix / name))

    def iter_level(self, level: int) -> Iterable[Tuple[Path, Tree]]:
        root = Path(())
        if level < 0:
            raise ValueError(f"negative level {level}")
        else:
            yield from self._iter_level(level, root)

    def _merge_one(self, other: Tree) -> Tree:
        if self.data != other.data:
            raise TreeCollision(f"data mismatch")
        name_collisions = set(self.children) & set(other.children)
        merged_trees: Mapping[Name, Tree] = {}
        for name in name_collisions:
            left = self.children[name]
            right = other.children[name]
            merged_trees[name] = left.merge(right)

        return Tree({**self.children, **other.children, **merged_trees}, self.data)

    def merge(self: Tree, *other: Tree) -> Tree:
        trees = [self, *other]
        return reduce(Tree._merge_one, trees)

    def __repr__(self):
        data_repr = "" if self.data is None else f", {repr(self.data)}"
        return f"Tree({repr(dict(self.children))}{data_repr})"
