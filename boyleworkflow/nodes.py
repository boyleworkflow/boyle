from __future__ import annotations
from boyleworkflow.frozendict import FrozenDict
from dataclasses import dataclass
from typing import (
    Collection,
    FrozenSet,
    Iterator,
    Literal,
    Mapping,
    Protocol,
    Tuple,
    runtime_checkable,
)
from boyleworkflow.loc import Name, Loc
from boyleworkflow.tree import Tree
from boyleworkflow.calc import Calc, Op


class AbstractNode(Protocol):
    inp: FrozenDict[Loc, AbstractNode]

    @property
    def parents(self) -> FrozenSet[AbstractNode]:
        ...

    @property
    def out_levels(self) -> Tuple[Name, ...]:
        ...

    @property
    def run_depth(self) -> int:
        ...

    def build_input(self, results: Mapping[AbstractNode, Tree]) -> Tree:
        ...


def _get_out_levels(nodes: Collection[AbstractNode]) -> Tuple[Name, ...]:
    nodes = set(nodes)
    if not nodes:
        return ()

    different_levels = {node.out_levels for node in nodes}
    if not len(different_levels) == 1:
        raise ValueError(f"input levels do not match: {different_levels}")

    (levels,) = different_levels
    return levels


@runtime_checkable
class AbstractCalcNode(AbstractNode, Protocol):
    def iter_calcs(self, node_input: Tree) -> Iterator[Tuple[Loc, Calc]]:
        ...


@runtime_checkable
class AbstractVirtualNode(AbstractNode, Protocol):
    def run_subtree(self, subtree: Tree) -> Tree:
        ...


@dataclass(frozen=True)
class Node(AbstractNode):
    inp: FrozenDict[Loc, AbstractNode]

    def __post_init__(self):
        self.inp_levels  # To raise an error if levels don't match

    @property
    def parents(self) -> FrozenSet[AbstractNode]:
        return frozenset(self.inp.values())

    @property
    def inp_levels(self) -> Tuple[Name, ...]:
        return _get_out_levels(self.parents)

    @property
    def out_levels(self) -> Tuple[Name, ...]:
        return self.inp_levels

    @property
    def run_depth(self) -> int:
        return len(self.out_levels)

    def build_input(self, results: Mapping[AbstractNode, Tree]) -> Tree:
        # TODO: check for agreement between node out_levels etc

        return Tree.merge(
            results[parent].map_level(self.run_depth, lambda tree: tree.nest(loc))
            for loc, parent in self.inp.items()
        )


@dataclass(frozen=True)
class CalcNode(Node, AbstractCalcNode):
    op: Op
    out: FrozenSet[Loc]

    def iter_calcs(self, node_input: Tree) -> Iterator[Tuple[Loc, Calc]]:
        for loc, subtree in node_input.iter_level(self.run_depth):
            yield loc, Calc(subtree, self.op, self.out)


@dataclass(frozen=True)
class VirtualNode(Node, AbstractVirtualNode):
    def run_subtree(self, subtree: Tree) -> Tree:
        ...


@dataclass(frozen=True)
class SplitNode(VirtualNode):
    level: Name

    def __post_init__(self):
        if self.level in self.inp_levels:
            raise ValueError(f"duplicate level '{self.level}'")

    @property
    def out_levels(self) -> Tuple[Name, ...]:
        return self.inp_levels + (self.level,)

    @property
    def run_depth(self) -> Literal[0]:
        return 0

    def run_subtree(self, subtree: Tree) -> Tree:
        return subtree

    @classmethod
    def from_node(cls, node: Node, level: Name):
        return cls(FrozenDict({Loc("."): node}), level)


@dataclass(frozen=True)
class PickNode(VirtualNode):
    loc: Loc

    def run_subtree(self, subtree: Tree) -> Tree:
        return subtree.pick(self.loc)

    @classmethod
    def from_node(cls, node: Node, loc: Loc):
        return cls(FrozenDict({Loc("."): node}), loc)


@dataclass(frozen=True)
class NoOpNode(VirtualNode):
    def run_subtree(self, subtree: Tree) -> Tree:
        return subtree

    @property
    def run_depth(self) -> Literal[0]:
        return 0


@dataclass(frozen=True)
class PutNode(NoOpNode):
    pass
