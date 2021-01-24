from __future__ import annotations
from boyleworkflow.frozendict import FrozenDict
from dataclasses import dataclass
from boyleworkflow.calc import Path, Op
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

# Allowing Collection[PathLike] is possible but opens for mistakes because
# str is also a Collection[PathLike]. So 'abc' could be confused with ['a', 'b', 'c']
PathLikePlural = Union[Tuple[PathLike], List[PathLike], AbstractSet[PathLike]]


def _ensure_path(value: PathLike) -> Path:
    if isinstance(value, str):
        return Path.from_string(value)
    else:
        return value


@dataclass(frozen=True)
class NodeBundle:
    inp: FrozenDict[Path, Node]
    op: Op
    out: FrozenSet[Path]

    @staticmethod
    def create(inp: Mapping[PathLike, Node], op: Op, out: PathLikePlural) -> NodeBundle:
        return NodeBundle(
            inp=FrozenDict({_ensure_path(path): node for path, node in inp.items()}),
            op=op,
            out=frozenset(map(_ensure_path, out)),
        )

    def __getitem__(self, key: PathLike) -> Node:
        return dict({node.out: node for node in self.nodes})[_ensure_path(key)]

    @property
    def nodes(self: NodeBundle) -> FrozenSet[Node]:
        return frozenset({Node(self, path) for path in self.out})


@dataclass(frozen=True)
class Node:
    bundle: NodeBundle
    out: Path

    @property
    def parents(self) -> FrozenSet[Node]:
        return frozenset(self.bundle.inp.values())

    @staticmethod
    def create(inp: Mapping[PathLike, Node], op: Op, out: PathLike) -> Node:
        return NodeBundle.create(inp, op, [out])[out]
