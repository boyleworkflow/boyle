from __future__ import annotations
from dataclasses import dataclass
from functools import reduce
from typing import Iterable, Mapping, Tuple, Union

_SEPARATOR = "/"
_DOT = "."
_DOUBLE_DOT = ".."
_FORBIDDEN_NAMES = ["", _DOT, _DOUBLE_DOT]


@dataclass(frozen=True)
class Name:
    value: str

    @classmethod
    def from_string(cls, value: str) -> Name:
        return Name(value)

    @classmethod
    def from_name_like(cls, value: NameLike) -> Name:
        if isinstance(value, Name):
            return value
        else:
            return Name(value)

    def __post_init__(self):
        if self.value in _FORBIDDEN_NAMES:
            raise ValueError(f"invalid Name: {repr(self.value)}")
        if _SEPARATOR in self.value:
            raise ValueError(f"invalid Name with separator: {self.value}")


@dataclass(frozen=True)
class Path:
    names: Tuple[Name, ...] = ()

    @classmethod
    def from_string(cls, value: str) -> Path:
        if value.endswith(_SEPARATOR):
            value = value[:-1]
        path_segments = (v for v in value.split(_SEPARATOR) if v != _DOT)
        return Path(tuple(map(Name.from_string, path_segments)))

    @classmethod
    def from_path_like(cls, value: PathLike) -> Path:
        if isinstance(value, Path):
            return value
        else:
            return Path.from_string(value)

    def __truediv__(self, name: Name) -> Path:
        return Path(self.names + (name,))

    @property
    def parent(self):
        return Path(self.names[:-1])


PathLike = Union[Path, str]
NameLike = Union[Name, str]


@dataclass
class Leaf:
    data: str


class TreeCollision(ValueError):
    pass


@dataclass
class Tree:
    children: Mapping[Name, TreeItem]

    def __getitem__(self, key: NameLike) -> TreeItem:
        return self.children[Name.from_name_like(key)]

    @classmethod
    def from_dict(cls, d: Mapping[NameLike, TreeItemLike]) -> Tree:
        return Tree(
            {
                Name.from_name_like(name): to_tree_item(value)
                for name, value in d.items()
            }
        )

    @classmethod
    def _from_nested_item(cls, path: Path, value: TreeItemLike) -> Tree:
        reversed_names = reversed(path.names)
        content = to_tree_item(value)
        tree = Tree({next(reversed_names): content})
        for name in reversed_names:
            tree = Tree({name: tree})
        return tree

    @classmethod
    def from_nested_items(cls, d: Mapping[PathLike, TreeItemLike]) -> Tree:
        trees = (
            Tree._from_nested_item(Path.from_path_like(path_like), value)
            for path_like, value in d.items()
        )
        return reduce(Tree.merge, trees)

    def _walk_prefixed(self, prefix: Path) -> Iterable[Tuple[Path, TreeItem]]:
        for k, v in self.children.items():
            if isinstance(v, Tree):
                yield from v._walk_prefixed(prefix / k)
            yield (prefix / k, v)

    def walk(self) -> Iterable[Tuple[Path, TreeItem]]:
        yield from self._walk_prefixed(Path(()))

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
LeafLike = Union[Leaf, str]
TreeLike = Union[Tree, Mapping[NameLike, Union["TreeLike", LeafLike]]]
TreeItemLike = Union[TreeLike, LeafLike]


def to_tree_item(value: TreeItemLike) -> TreeItem:
    if isinstance(value, (Tree, Leaf)):
        return value
    elif isinstance(value, str):
        return Leaf(value)
    else:
        return Tree.from_dict(value)
