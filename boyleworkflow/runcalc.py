from __future__ import annotations
from boyleworkflow.frozendict import FrozenDict
from functools import partial
from boyleworkflow.loc import Loc
from boyleworkflow.tree import Tree
from dataclasses import dataclass
from typing import Mapping, Optional
from boyleworkflow.calc import Calc, CalcOut, Env, run_calc
from boyleworkflow.graph import EnvNode, Node, VirtualNode
from boyleworkflow.log import CacheLog, NotFound, Run


@dataclass
class NodeRunner:
    env: Env
    log: Optional[CacheLog] = None

    def run(self, node: Node, results: Mapping[Node, Tree]) -> Tree:
        input_tree = self._build_input_tree(node, results)
        subtree_runner = self._get_subtree_runner(node)
        result = input_tree.map_level(node.run_depth, subtree_runner)
        return result

    def recall(self, node: Node, results: Mapping[Node, Tree]) -> Optional[Tree]:
        if not self.log:
            return None

        if isinstance(node, EnvNode):
            try:
                return self._recall_env_node(node, results)
            except NotFound:
                return None

    def can_restore(self, tree: Tree) -> bool:
        return self.env.can_restore(tree)

    def _build_input_tree(self, node: Node, results: Mapping[Node, Tree]):
        return Tree.merge(
            results[parent].map_level(node.run_depth, lambda tree: tree.nest(loc))
            for loc, parent in node.inp.items()
        )

    def _get_subtree_runner(self, node: Node):
        if isinstance(node, VirtualNode):
            return node.run_subtree
        elif isinstance(node, EnvNode):
            return partial(self._run_env_node_subtree, node)

        raise ValueError(f"unknown node type {type(node)}")

    def _run_env_node_subtree(self, node: EnvNode, subtree: Tree) -> Tree:
        calc = Calc(subtree, node.op, node.out)
        results = run_calc(calc, self.env)
        self._store_calc_results(calc, results)
        return Tree.from_nested_items(results)

    def _store_calc_results(self, calc: Calc, results: Mapping[Loc, Tree]):
        if not self.log:
            return

        run = Run(calc, FrozenDict(results))
        self.log.save_run(run)


    def _recall_env_node(self, node: EnvNode, results: Mapping[Node, Tree]) -> Tree:
        input_tree = self._build_input_tree(node, results)
        recall_subtree = partial(self._recall_env_node_subtree, node)
        return input_tree.map_level(node.run_depth, recall_subtree)

    def _recall_env_node_subtree(self, node: EnvNode, subtree: Tree) -> Tree:
        return Tree.from_nested_items(
            {
                loc: self._recall_calc_out(CalcOut(subtree, node.op, loc))
                for loc in node.out
            }
        )

    def _recall_calc_out(self, calc_out: CalcOut) -> Tree:
        assert self.log
        return self.log.recall_result(calc_out)
