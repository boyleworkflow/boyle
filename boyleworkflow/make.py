from boyleworkflow.scheduling import GraphState
from boyleworkflow.nodes import Node
from typing import Mapping
from boyleworkflow.calc import Calc, Env, Loc, Op, Result, run


# TODO: await resolution of https://github.com/microsoft/pyright/issues/1326


def _run_priority_work(
    state: GraphState[Node[Op]], env: Env[Op]
) -> Mapping[Node[Op], Result]:
    results = {}
    node_bundles = {node.bundle for node in state.priority_work}
    for bundle in node_bundles:
        calc = Calc(
            {loc: state.results[parent] for loc, parent in bundle.inp.items()},
            bundle.op,
            bundle.out,
        )
        calc_results = run(calc, env)
        for node in bundle.nodes & state.priority_work:
            results[node] = calc_results[node.out]
    return results


def _advance_state(
    state: GraphState[Node[Op]], env: Env[Op]
) -> GraphState[Node[Op]]:
    results = _run_priority_work(state, env)
    return state.add_results(results).add_restorable(results)


def make(requested: Mapping[Loc, Node[Op]], env: Env[Op]):
    state = GraphState.from_requested(requested.values())
    while state.priority_work:
        state = _advance_state(state, env)
    for loc, node in requested.items():
        env.deliver(loc, state.results[node])
