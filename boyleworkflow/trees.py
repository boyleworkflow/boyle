from boyleworkflow.util import unique_json_digest
from typing import (
    Iterable,
    Mapping,
    NewType,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)
from functools import reduce

Name = NewType("Name", str)
Key = Tuple[Name]

Tree = NewType("Tree", Mapping[Name, "TreeItem"])
Leaf = NewType("Leaf", str)
TreeItem = Union[Tree, Leaf]

TreeId = NewType("TreeId", str)


def calc_tree_id(tree: Tree) -> TreeId:
    return TreeId(unique_json_digest(tree))


T = TypeVar("T")
Indexed = Iterable[Tuple[Key, T]]


def _merge(a: Tree, b: Tree) -> Tree:
    ...


def merge_trees(*trees: Tree) -> Tree:
    return reduce(_merge, trees)


def iter_aligned(vectors: Sequence[Indexed[Tree]]) -> Indexed[Sequence[Tree]]:
    for inputs in zip(*vectors):
        keys, trees = cast(Tuple[Tuple[Key], Tuple[Tree]], zip(*inputs))

        unique_keys = set(keys)
        if len(unique_keys) > 1:
            raise ValueError(f"Misaligned inputs: {inputs}")
        (key,) = unique_keys

        yield key, trees


def iter_merged(vectors: Sequence[Indexed[Tree]]) -> Indexed[Tree]:
    for key, trees in iter_aligned(vectors):
        yield key, merge_trees(*trees)


class TreeConflictException(ValueError):
    pass


# @attr.s(auto_attribs=True)
# class Tree:
#     subtrees: Mapping[Name, Tree]
#     blobs: Mapping[Name, Digest]

#     tree_id: TreeId = attr.ib(init=False)

#     def __attrs_post_init__(self):
#         name_collisions = set(self.subtrees) & set(self.blobs)
#         if name_collisions:
#             raise ValueError(
#                 f"Name collisions between subtrees and blobs {name_collisions}"
#             )

#         tree_id_items = dict(
#             subtrees={n: t.tree_id for n, s in self.subtrees.items()},
#             blobs=self.blobs,
#         )
#         self.tree_id = TreeId(unique_json_digest(tree_id_items))

#     def merge(self, other: Tree) -> Tree:
#         blob_collisions = set(self.blobs) & set(other.blobs)
#         if blob_collisions:
#             raise ValueError(
#                 f"Both trees have blob(s) called {blob_collisions}"
#             )
#         merged_blobs = {**self.blobs, **other.blobs}

#         merged_subtrees = self.subtrees.copy()

#         for name, other_subtree in other.subtrees.items():
#             self_subtree = self.subtrees.get(name, None)
#             merged_subtrees[name] = (
#                 self_subtree.merge(other_subtree)
#                 if self_subtree
#                 else other_subtree
#             )

#         return Tree(merged_subtrees, merged_blobs)