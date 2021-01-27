from boyleworkflow.scheduling import GraphState
from boyleworkflow.nodes import Node, NodeBundle
from typing import Mapping
from boyleworkflow.calc import Env, run
from boyleworkflow.tree import Path, Tree


NodeResults = Mapping[Node, Tree]


def _run_node_bundle(
    node_bundle: NodeBundle, results: NodeResults, env: Env
) -> NodeResults:
    calc_bundles = node_bundle.build_calc_bundles(results)

    node_bundle_results = Tree.from_nested_items(
        {
            index: Tree.from_nested_items(run(cb, env))
            for index, cb in calc_bundles.items()
        }
    )
    return node_bundle.extract_node_results(node_bundle_results)


def _run_priority_work(state: GraphState, env: Env) -> NodeResults:
    nodes = state.priority_work
    node_bundles = {node.bundle for node in nodes}
    all_results = {}
    for node_bundle in node_bundles:
        all_results.update(_run_node_bundle(node_bundle, state.results, env))
    requested_results = {n: all_results[n] for n in nodes}
    return requested_results


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
