from __future__ import annotations
from dataclasses import dataclass
from functools import reduce
from typing import Iterable, Mapping, Tuple, Union

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


@dataclass(frozen=True)
class Leaf:
    data: str


class TreeCollision(ValueError):
    pass


@dataclass
class Tree:
    children: Mapping[Name, TreeItem]

    def __getitem__(self, key: Name) -> TreeItem:
        return self.children[key]

    def pick(self, path: Path) -> TreeItem:
        result = self
        for name in path.names:
            if isinstance(result, Leaf):
                raise ValueError(f"{path} too long ({name} is a leaf)")
            try:
                result = result[name]
            except KeyError:
                raise ValueError(f"no item {name} found")
        return result

    @classmethod
    def _from_nested_item(cls, path: Path, item: TreeItem) -> Tree:
        reversed_names = reversed(path.names)
        tree = Tree({next(reversed_names): item})
        for name in reversed_names:
            tree = Tree({name: tree})
        return tree

    @classmethod
    def from_nested_items(cls, d: Mapping[Path, TreeItem]) -> Tree:
        empty_tree = Tree({})
        trees = (Tree._from_nested_item(path, item) for path, item in d.items())
        return reduce(Tree.merge, trees, empty_tree)

    def _walk_prefixed(self, prefix: Path) -> Iterable[Tuple[Path, TreeItem]]:
        for k, v in self.children.items():
            if isinstance(v, Tree):
                yield from v._walk_prefixed(prefix / k)
            yield (prefix / k, v)

    def walk(self) -> Iterable[Tuple[Path, TreeItem]]:
        yield from self._walk_prefixed(Path(()))

    def _iter_level(self, level: int, prefix: Path) -> Iterable[Tuple[Path, TreeItem]]:
        # here we can assume level >= 1
        if not self.children:
            raise ValueError(f"empty subtree encountered at {prefix}")

        if level == 1:
            # if level == 1 we yield all the children which may be Tree or Leaf
            for name, value in self.children.items():
                yield (prefix / name), value
        elif level > 1:
            # We are supposed to descend further, and all children must be Tree
            for name, value in self.children.items():
                if isinstance(value, Leaf):
                    raise ValueError(f"leaf encountered at {prefix / name}")
                yield from value._iter_level(level - 1, prefix / name)
        else:
            raise RuntimeError(f"how did this happen? level={level}")

    def iter_level(self, level: int) -> Iterable[Tuple[Path, TreeItem]]:
        root = Path(())
        if level < 0:
            raise ValueError(f"negative level {level}")
        elif level == 0:
            yield root, self
        else:
            yield from self._iter_level(level, root)

    def _merge_one(self, other: Tree) -> Tree:
        name_collisions = set(self.children) & set(other.children)
        merged_trees: Mapping[Name, Tree] = {}
        for name in name_collisions:
            left = self.children[name]
            right = other.children[name]
            if isinstance(left, Tree) and isinstance(right, Tree):
                merged_trees[name] = left.merge(right)
            else:
                if left != right:
                    raise TreeCollision(name)

        return Tree({**self.children, **other.children, **merged_trees})

    def merge(self: Tree, *other: Tree) -> Tree:
        trees = [self, *other]
        return reduce(Tree._merge_one, trees)


TreeItem = Union[Tree, Leaf]
