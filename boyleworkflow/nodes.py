from __future__ import annotations
from itertools import groupby
from dataclasses import dataclass
from typing import Iterable, Sequence, Tuple, Union, cast
from boyleworkflow.trees import Name, Key, Tree, Indexed, iter_merged
from boyleworkflow.calcs import Op, Glob

NodeIndex = Tuple[Union[Name, slice], ...]

@dataclass
class Node:
    op: Op
    parents: Sequence[Node]
    out_glob: Glob

    def gen_inputs(self, inp_vectors: Sequence[Indexed[Tree]]) -> Indexed[Tree]:
        raise NotImplementedError()

    def __getitem__(self, key: NodeIndex) -> Node:
        raise NotImplementedError()


class MapNode(Node):
    def gen_inputs(self, inp_vectors: Sequence[Indexed[Tree]]) -> Indexed[Tree]:
        yield from iter_merged(inp_vectors)

    def __getitem__(self, key: NodeIndex) -> Node:
        subscripted_parents = [parent[key] for parent in self.parents]
        return MapNode(self.op, subscripted_parents, self.out_glob)


class ReduceNode(Node):
    def gen_inputs(
        self, inp_vectors: Sequence[Iterable[Tuple[Key, Tree]]]
    ) -> Iterable[Tuple[Key, Tree]]:
        merged = iter_merged(inp_vectors)

        def get_group_key(pair: Tuple[Key, Tree]) -> Key:
            key, _ = pair
            return key

        for group_key, keys_and_subtrees in groupby(merged, key=get_group_key):
            keys, subtrees = zip(*keys_and_subtrees)
            keys = cast(Tuple[Key], keys)
            subtrees = cast(Tuple[Tree], subtrees)
            subtree_names = (key[-1] for key in keys)
            group_tree = Tree(
                {
                    subtree_name: subtree
                    for subtree_name, subtree in zip(subtree_names, subtrees)
                }
            )
            yield group_key, group_tree

    def __getitem__(self, key: NodeIndex) -> Node:
        parent_index = (*key, slice(None))
        subscripted_parents = [parent[parent_index] for parent in self.parents]
        return ReduceNode(self.op, subscripted_parents, self.out_glob)
