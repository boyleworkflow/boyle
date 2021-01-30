from __future__ import annotations
from boyleworkflow.tree import Name, Tree
from boyleworkflow.frozendict import FrozenDict
import dataclasses
from dataclasses import dataclass
from boyleworkflow.calc import Calc, Path, Op
from typing import (
    AbstractSet,
    Collection,
    FrozenSet,
    List,
    Mapping,
    Tuple,
    Union,
)

PathLike = Union[Path, str]
NameLike = Union[Name, str]

# Allowing Collection[PathLike] is possible but opens for mistakes because
# str is also a Collection[PathLike]. So 'abc' could be confused with ['a', 'b', 'c']
PathLikePlural = Union[Tuple[PathLike], List[PathLike], AbstractSet[PathLike]]


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


@dataclass(frozen=True)
class Task:
    inp: FrozenDict[Path, Node]
    op: Op
    out: FrozenSet[Path]
    levels: Tuple[Name, ...] = ()

    @property
    def depth(self) -> int:
        return len(self.levels)

    @staticmethod
    def create(inp: Mapping[PathLike, Node], op: Op, out: PathLikePlural) -> Task:
        return Task(
            inp=FrozenDict({_ensure_path(path): node for path, node in inp.items()}),
            op=op,
            out=frozenset(map(_ensure_path, out)),
        )

    def __getitem__(self, key: PathLike) -> Node:
        return dict({node.out: node for node in self.nodes})[_ensure_path(key)]

    @property
    def nodes(self: Task) -> FrozenSet[Node]:
        return frozenset({Node(self, path) for path in self.out})

    def descend(self, level_name: NameLike):
        converted_name = _ensure_name(level_name)
        if converted_name in self.levels:
            raise ValueError(f"duplicate level name {converted_name}")
        return dataclasses.replace(self, levels=self.levels + (converted_name,))

    def ascend(self):
        if not self.levels:
            raise ValueError("cannot ascend non-nested")
        return dataclasses.replace(self, levels=self.levels[:-1])

    def _build_input_tree(self, results: Mapping[Node, Tree]) -> Tree:
        return Tree.merge(
            results[inp_node].map_level(self.depth, Tree.nest, inp_path)
            for inp_path, inp_node in self.inp.items()
        )

    def build_calcs(self, results: Mapping[Node, Tree]) -> Mapping[Path, Calc]:
        inp_tree = self._build_input_tree(results)
        return {
            index: Calc(calc_inp, self.op, self.out)
            for index, calc_inp in inp_tree.iter_level(self.depth)
        }

    def extract_node_results(self, task_results: Tree) -> Mapping[Node, Tree]:
        return {
            node: task_results.map_level(self.depth, Tree.pick, node.out)
            for node in self.nodes
        }


@dataclass(frozen=True)
class Node:
    task: Task
    out: Path

    @property
    def parents(self) -> FrozenSet[Node]:
        return frozenset(self.task.inp.values())

    @staticmethod
    def create(inp: Mapping[PathLike, Node], op: Op, out: PathLike) -> Node:
        return Task.create(inp, op, [out])[out]

    def descend(self, level_name: NameLike):
        return self.task.descend(level_name)[self.out]

    def ascend(self):
        return self.task.ascend()[self.out]
