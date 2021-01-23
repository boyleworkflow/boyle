from boyleworkflow.scheduling import GraphState
from boyleworkflow.nodes import Node
from typing import Mapping
from boyleworkflow.calc import Calc, Env, run
from boyleworkflow.tree import Path, Tree, TreeItem


def _run_priority_work(state: GraphState, env: Env) -> Mapping[Node, TreeItem]:
    results = {}
    node_bundles = {node.bundle for node in state.priority_work}
    for bundle in node_bundles:
        calc_input = Tree.from_nested_items(
            {path: state.results[parent] for path, parent in bundle.inp.items()}
        )
        calc = Calc(
            calc_input,
            bundle.op,
            bundle.out,
        )
        calc_results = run(calc, env)
        for node in bundle.nodes & state.priority_work:
            results[node] = calc_results.pick(node.out)
    return results


def _advance_state(state: GraphState, env: Env) -> GraphState:
    results = _run_priority_work(state, env)
    return state.add_results(results).add_restorable(results)


def make(requested: Mapping[Path, Node], env: Env):
    state = GraphState.from_requested(requested.values())
    while state.priority_work:
        state = _advance_state(state, env)
    result_tree = Tree.from_nested_items(
        {path: state.results[node] for path, node in requested.items()}
    )
    env.deliver(result_tree)
