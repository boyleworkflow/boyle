from __future__ import annotations
from dataclasses import dataclass
from boyleworkflow.calc import Path, Op
from typing import (
    Collection,
    FrozenSet,
    Mapping,
    Set,
)


@dataclass(frozen=True)
class NodeBundle:
    inp: Mapping[Path, Node]
    op: Op
    out: FrozenSet[Path]

    @property
    def nodes(self: NodeBundle) -> FrozenSet[Node]:
        return frozenset({Node(self, path) for path in self.out})

    def __hash__(self):
        return hash((tuple(sorted(self.inp.items())), self.op, self.out))


@dataclass(frozen=True)
class Node:
    bundle: NodeBundle
    out: Path

    @property
    def parents(self) -> FrozenSet[Node]:
        return frozenset(self.bundle.inp.values())


def create_simple_node(inp: Mapping[str, Node], op: Op, out: str) -> Node:
    out_path = Path.from_string(out)
    node = NodeBundle(
        {Path.from_string(k): v for k, v in inp.items()},
        op,
        frozenset([out_path]),
    )
    return Node(node, out_path)


def create_sibling_nodes(
    inp: Mapping[str, Node], op: Op, out: Collection[str]
) -> Set[Node]:
    out_paths = frozenset(map(Path.from_string, out))
    node = NodeBundle(
        {Path.from_string(k): v for k, v in inp.items()}, op, out_paths
    )
    return {Node(node, loc) for loc in out_paths}
