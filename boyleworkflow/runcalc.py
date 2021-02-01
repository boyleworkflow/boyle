from __future__ import annotations
from functools import partial
from boyleworkflow.tree import Tree
from dataclasses import dataclass
from typing import Mapping, Optional
from boyleworkflow.calc import Calc, Env, run_calc
from boyleworkflow.graph import EnvNode, Node, VirtualNode
from boyleworkflow.log import CacheLog


@dataclass
class RunSystem:
    env: Env
    log: Optional[CacheLog] = None

    def run(self, node: Node, results: Mapping[Node, Tree]) -> Tree:
        input_tree = Tree.merge(
            results[parent].map_level(node.run_depth, lambda tree: tree.nest(path))
            for path, parent in node.inp.items()
        )
        subtree_runner = self._get_subtree_runner(node)
        return input_tree.map_level(node.run_depth, subtree_runner)

    def _get_subtree_runner(self, node: Node):
        if isinstance(node, VirtualNode):
            return node.run_subtree
        elif isinstance(node, EnvNode):
            return partial(self._run_env_node_subtree, node)

        raise ValueError(f"unknown node type {type(node)}")

    def _run_env_node_subtree(self, node: EnvNode, subtree: Tree) -> Tree:
        calc = Calc(subtree, node.op, node.out)
        return Tree.from_nested_items(run_calc(calc, self.env))
