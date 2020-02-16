from typing import Set, Sequence, Mapping, Iterable, Tuple
import contextlib
import os
from pathlib import Path
import datetime

import attr

from boyleworkflow.core import (
    Op,
    Calc,
    Node,
    Tree,
    Run,
    NodeResult,
    Log,
    Loc,
    Storage,
    NotFoundException,
)
from boyleworkflow.ops import run_op


@attr.s(auto_attribs=True)
class Context:
    log: Log
    storage: Storage
    project_dir: Loc
    temp_dir: Loc
    outdata_dir: Loc


def _run_calc(calc: Calc, ctx: Context):
    start_time = datetime.datetime.utcnow()
    output_tree = run_op(calc.op, calc.input_tree, ctx.storage)
    end_time = datetime.datetime.utcnow()
    run = Run(calc, output_tree, start_time, end_time)
    ctx.log.save_run(run)


def _get_upstream_sorted(defns: Iterable[Node]) -> Sequence[Node]:
    raise NotImplementedError()

def _build_tree_from_subtrees(subtrees: Mapping[Loc, Tree]) -> Tree:
    raise NotImplementedError()

def _generate_calcs(node: Node, ctx: Context) -> Iterable[Tuple[Loc, Calc]]:
    raise NotImplementedError()


def _ensure_node_restorable(node: Node, ctx: Context):
    # The requested Nodes are also later marked as explicit requests,
    # when they are placed in the out directory.
    # Here, just mark the implicit requests.
    explicit_request = False

    subtrees = {}
    for loc, calc in _generate_calcs(node, ctx):

        needs_run = True  # assume until proven otherwise

        with contextlib.suppress(NotFoundException):
            tree = ctx.log.get_calc_result(calc, node.outputs)

            if ctx.storage.can_restore(tree):
                needs_run = False

        if needs_run:
            _run_calc(calc, ctx)
            tree = ctx.log.get_calc_result(calc, node.outputs)

        subtrees[loc] = tree

    tree = _build_tree_from_subtrees(subtrees)

    ctx.log.save_node_result(NodeResult(node, tree, explicit_request))


def _ensure_all_restorable(requested: Iterable[Node], ctx: Context):
    upstream_sorted = _get_upstream_sorted(requested)

    for node in upstream_sorted:
        _ensure_node_restorable(node, ctx)


def _get_out_loc(ctx: Context, name: str) -> Loc:
    return ctx.outdata_dir / name


def _place_results(request: Mapping[str, Node], ctx: Context):
    explicit_request = True
    for name, node in request.items():
        tree = ctx.log.get_node_result(node)

        ctx.log.save_node_result(NodeResult(node, tree, explicit_request))

        out_loc = _get_out_loc(ctx, name)
        ctx.storage.restore(tree, out_loc)


def make(request: Mapping[str, Node], ctx: Context):
    requested_nodes = set(request.values())
    _ensure_all_restorable(requested_nodes, ctx)
    _place_results(request, ctx)
