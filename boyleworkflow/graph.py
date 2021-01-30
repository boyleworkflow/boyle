from __future__ import annotations
import dataclasses
from dataclasses import dataclass
from typing import (
    AbstractSet,
    FrozenSet,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)
from boyleworkflow.tree import Name, Path, Tree
from boyleworkflow.frozendict import FrozenDict
from boyleworkflow.calc import Calc, Op, NO_OP

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


def _get_common_inp_level(inp: Mapping[Path, Node]):
    different_inp_levels = {inp_node.task.out_levels for inp_node in inp.values()}
    if not len(different_inp_levels) == 1:
        raise ValueError(f"input levels do not match: {different_inp_levels}")

    (inp_levels,) = different_inp_levels
    return inp_levels


@dataclass(frozen=True, init=False)
class Task:
    inp: FrozenDict[Path, Node]
    op: Op
    out: FrozenSet[Path]
    out_levels: Tuple[Name, ...]

    def __init__(
        self,
        inp: Union[Mapping[PathLike, Node], Mapping[Path, Node]],
        op: Op,
        out: PathLikePlural,
        out_levels: Optional[NameLikePlural] = None,
    ):
        inp_converted = FrozenDict(
            {_ensure_path(path): node for path, node in inp.items()}
        )
        if inp_converted:
            inp_levels = _get_common_inp_level(inp_converted)
        else:
            inp_levels: Tuple[Name, ...] = ()

        out_converted = frozenset(map(_ensure_path, out))

        out_levels_converted = (
            tuple(map(_ensure_name, out_levels))
            if out_levels is not None
            else inp_levels
        )

        attributes = {
            "inp": inp_converted,
            "op": op,
            "out": out_converted,
            "out_levels": out_levels_converted,
        }

        for name, value in attributes.items():
            object.__setattr__(self, name, value)

    @property
    def inp_levels(self) -> Tuple[Name, ...]:
        return _get_common_inp_level(self.inp)

    @property
    def depth(self) -> int:
        return len(self.out_levels)

    def __getitem__(self, key: PathLike) -> Node:
        path = _ensure_path(key)
        if path not in self.out:
            raise ValueError(f"no output defined at {path}")
        return Node(self, path)

    @property
    def nodes(self: Task) -> FrozenSet[Node]:
        return frozenset({Node(self, path) for path in self.out})

    def descend(self, level_name: NameLike):
        converted_name = _ensure_name(level_name)
        if converted_name in self.out_levels:
            raise ValueError(f"duplicate level name {converted_name}")
        return Task(
            {path: self[path] for path in self.out},
            NO_OP,
            self.out,
            out_levels=self.out_levels + (converted_name,),
        )

    def ascend(self):
        if not self.out_levels:
            raise ValueError("cannot ascend non-nested")
        return Task(
            {path: self[path] for path in self.out},
            NO_OP,
            self.out,
            out_levels=self.out_levels[:-1],
        )

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
        return Task(inp, op, [out])[out]

    def descend(self, level_name: NameLike):
        return self.task.descend(level_name)[self.out]

    def ascend(self):
        return self.task.ascend()[self.out]
