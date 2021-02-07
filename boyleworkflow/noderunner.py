from __future__ import annotations
from boyleworkflow.calc import Env, run_calc
from boyleworkflow.tree import Tree
from dataclasses import dataclass
from boyleworkflow.nodes import AbstractNode, AbstractCalcNode, AbstractVirtualNode
from boyleworkflow.log import Log, NotFound, Run
from boyleworkflow.calc import Calc, run_calc


class CannotRecall(Exception):
    pass


@dataclass(frozen=True)
class NodeRunner:
    env: Env
    log: Log

    def ensure_restorable(self, node: AbstractNode, node_input: Tree):
        if isinstance(node, AbstractVirtualNode):
            return  # because AbstractVirtualNode can be restored if its parents can

        elif isinstance(node, AbstractCalcNode):
            for _, calc in node.iter_calcs(node_input):
                try:
                    result = self.log.recall_result(calc)
                    if self.can_restore(result):
                        continue
                except NotFound:
                    self._run_calc(calc)
            return

        raise ValueError(f"Unexpected type of node: '{type(node)}")

    def _run_calc(self, calc: Calc):
        result = run_calc(calc, self.env)
        run = Run(calc, result)
        self.log.save_run(run)

    def recall(self, node: AbstractNode, node_input: Tree) -> Tree:
        if isinstance(node, AbstractVirtualNode):
            return node_input.map_level(node.run_depth, node.run_subtree)

        elif isinstance(node, AbstractCalcNode):
            try:
                calc_results = {
                    loc: self.log.recall_result(calc)
                    for loc, calc in node.iter_calcs(node_input)
                }
                return Tree.from_nested_items(calc_results)
            except NotFound as e:
                raise CannotRecall from e

        raise ValueError(f"Unexpected type of node: '{type(node)}")

    def can_restore(self, result: Tree) -> bool:
        return self.env.can_restore(result)
