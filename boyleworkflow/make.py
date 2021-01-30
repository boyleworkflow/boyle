from boyleworkflow.scheduling import GraphState
from boyleworkflow.nodes import Node, Task
from typing import Mapping
from boyleworkflow.calc import Env, run
from boyleworkflow.tree import Path, Tree


NodeResults = Mapping[Node, Tree]


def _run_task(task: Task, results: NodeResults, env: Env) -> NodeResults:
    calcs = task.build_calcs(results)

    task_results = Tree.from_nested_items(
        {index: Tree.from_nested_items(run(calc, env)) for index, calc in calcs.items()}
    )
    return task.extract_node_results(task_results)


def _run_priority_work(state: GraphState, env: Env) -> NodeResults:
    nodes = state.priority_work
    tasks = {node.task for node in nodes}
    all_results = {}
    for task in tasks:
        all_results.update(_run_task(task, state.results, env))
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
