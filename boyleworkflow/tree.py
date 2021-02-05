from __future__ import annotations
from dataclasses import dataclass
from functools import reduce
from typing import (
    Callable,
    Iterable,
    Iterator,
    Mapping,
    Tuple,
)
from boyleworkflow.frozendict import FrozenDict
from boyleworkflow.util import get_id_str, FrozenJSON, JSONData, freeze
from boyleworkflow.loc import Loc, Name


class TreeCollision(ValueError):
    pass


TreeData = FrozenJSON


@dataclass(frozen=True, init=False)
class Tree(Mapping[Name, "Tree"]):
    tree_id: str
    _children: FrozenDict[Name, Tree]
    data: TreeData

    def __init__(self, children: Mapping[Name, Tree], data: JSONData = None):
        object.__setattr__(self, "_children", FrozenDict(children))
        object.__setattr__(self, "data", data)
        object.__setattr__(
            self,
            "tree_id",
            get_id_str(
                type(self),
                {
                    "children": {
                        name.value: tree.tree_id
                        for name, tree in self._children.items()
                    },
                    "data": freeze(self.data),
                },
            ),
        )

    def __getitem__(self, key: Name) -> Tree:
        return self._children[key]

    def __iter__(self) -> Iterator[Name]:
        return iter(self._children)

    def __len__(self) -> int:
        return len(self._children)

    def pick(self, loc: Loc) -> Tree:
        result = self
        for name in loc.names:
            try:
                result = result[name]
            except KeyError:
                raise ValueError(f"no item {name} found")
        return result

    def nest(self, loc: Loc) -> Tree:
        if not loc.names:
            return self
        reversed_names = reversed(loc.names)
        tree = self
        for name in reversed_names:
            tree = Tree({name: tree})
        return tree

    @classmethod
    def from_nested_items(cls, subtrees: Mapping[Loc, Tree]) -> Tree:
        trees = (subtree.nest(loc) for loc, subtree in subtrees.items())
        return Tree.merge(trees)

    def _walk_prefixed(self, prefix: Loc) -> Iterable[Tuple[Loc, Tree]]:
        yield (prefix, self)
        for k, v in self.items():
            yield from v._walk_prefixed(prefix / k)

    def walk(self) -> Iterable[Tuple[Loc, Tree]]:
        yield from self._walk_prefixed(Loc())

    def _iter_level(self, level: int, prefix: Loc) -> Iterable[Tuple[Loc, Tree]]:
        if level == 0:
            yield prefix, self
        else:
            if not len(self):
                raise ValueError(f"no children found below {prefix}")
            for name, subtree in self.items():
                yield from subtree._iter_level(level - 1, (prefix / name))

    def iter_level(self, level: int) -> Iterable[Tuple[Loc, Tree]]:
        root = Loc()
        if level < 0:
            raise ValueError(f"negative level {level}")
        else:
            yield from self._iter_level(level, root)

    def map_level(
        self,
        level: int,
        func: Callable[[Tree], Tree],
    ) -> Tree:
        return Tree.from_nested_items(
            {loc: func(subtree) for loc, subtree in self.iter_level(level)}
        )

    def _merge_one(self, other: Tree, loc: Loc) -> Tree:
        if self.data != other.data:
            raise TreeCollision(f"data mismatch at {loc}")
        name_collisions = set(self) & set(other)
        merged_trees: Mapping[Name, Tree] = {}
        for name in name_collisions:
            left = self[name]
            right = other[name]
            merged_trees[name] = Tree._merge([left, right], loc / name)

        return Tree({**self, **other, **merged_trees}, self.data)

    @staticmethod
    def _merge(trees: Iterable[Tree], loc: Loc):
        trees = tuple(trees)
        if not trees:
            return Tree({})
        return reduce(lambda a, b: a._merge_one(b, loc), trees)

    @staticmethod
    def merge(trees: Iterable[Tree]) -> Tree:
        return Tree._merge(trees, Loc())

    def __repr__(self):
        data_repr = "" if self.data is None else f", {repr(self.data)}"
        return f"Tree({repr(dict(self))}{data_repr})"
