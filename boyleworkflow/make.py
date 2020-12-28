from boyleworkflow.scheduling import GraphState
from boyleworkflow.nodes import Node
from typing import Iterable, Mapping
from boyleworkflow.calc import Calc, Env, Loc, run


def _advance_state(state: GraphState, env: Env) -> GraphState:
    results = {}
    for node in state.priority_work:
        calc = Calc(
            {loc: state.results[parent] for loc, parent in node.inp.items()},
            node.op,
            [node.out],
        )
        results[node] = run(calc, env)[node.out]
    return state.add_results(results).add_restorable(results)


def make(requested: Mapping[Loc, Node], env: Env):
    state = GraphState.from_requested(requested.values())
    while state.priority_work:
        state = _advance_state(state, env)
    for loc, node in requested.items():
        env.deliver(loc, state.results[node])
