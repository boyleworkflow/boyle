from typing import Iterable, Tuple, TypeVar
from boyleworkflow.core import Context, NotFoundException
from boyleworkflow.trees import Key, Tree, Indexed
from boyleworkflow.calcs import Calc
from boyleworkflow.nodes import Node


def ensure_restorable(node: Node, ctx: Context) -> None:
    for key, tree in gen_results(node, ctx):
        if not ctx.storage.can_restore(tree):
            run(node[key], ctx)


def run(node: Node, ctx: Context) -> None:
    for parent in node.parents:
        ensure_restorable(parent, ctx)

    for calc in gen_calcs(node, ctx):
        # TODO record metadata
        # TODO actually run the calc
        ...


def gen_calcs(node: Node, ctx: Context) -> Indexed[Calc]:
    parent_results = [gen_results(parent, ctx) for parent in node.parents]
    for key, tree in node.gen_inputs(parent_results):
        yield key, Calc(tree, node.op)


def gen_results(node: Node, ctx: Context) -> Iterable[Tuple[Key, Tree]]:
    for key, inp_tree in gen_calcs(node, ctx):
        calc = Calc(inp_tree, node.op)
        try:
            result = ctx.log.search_result(calc, node.out_glob)
        except NotFoundException:
            run(node[key], ctx)
            result = ctx.log.search_result(calc, node.out_glob)

        yield from result
