from boyleworkflow.loc import Loc
from boyleworkflow.tree import Tree
from boyleworkflow.log import CalcOut
from boyleworkflow.frozendict import FrozenDict
from tests.util import tree_from_dict


def test_tree_id_depends_on_data():
    trees = [
        Tree({}),
        Tree({}, "data"),
    ]

    tree_ids = {tree.tree_id for tree in trees}

    assert len(tree_ids) == 2

def test_tree_id_depends_on_keys():
    trees = [
        tree_from_dict({"a": "x"}),
        tree_from_dict({"b": "x"}),
    ]

    tree_ids = {tree.tree_id for tree in trees}

    assert len(tree_ids) == 2

def test_tree_id_depends_on_children():
    trees = [
        tree_from_dict({"a": "x"}),
        tree_from_dict({"a": "y"}),
    ]

    tree_ids = {tree.tree_id for tree in trees}

    assert len(tree_ids) == 2

def test_tree_id_does_not_depend_on_child_order():
    trees = [
        tree_from_dict({"a": "x", "b": "y"}),
        tree_from_dict({"b": "y", "a": "x"}),
    ]

    tree_ids = {tree.tree_id for tree in trees}

    assert len(tree_ids) == 1

def test_calc_out_id_depends_on_inp():
    op = "op"
    out = Loc("out")
    objs = [
        CalcOut(tree_from_dict({}), op, out),
        CalcOut(tree_from_dict({"a": "b"}), op, out),
    ]

    ids = {obj.calc_out_id for obj in objs}

    assert len(ids) == 2

def test_calc_out_id_depends_on_op():
    inp = Tree({})
    out = Loc("out")
    objs = [
        CalcOut(inp, "op1", out),
        CalcOut(inp, "op2", out),
    ]

    ids = {obj.calc_out_id for obj in objs}

    assert len(ids) == 2

def test_calc_out_id_depends_on_out():
    inp = Tree({})
    op = "op"
    objs = [
        CalcOut(inp, op, Loc("out1")),
        CalcOut(inp, op, Loc("out2")),
    ]

    ids = {obj.calc_out_id for obj in objs}

    assert len(ids) == 2

def test_calc_out_op_can_be_frozen_dict():
    op = FrozenDict({})
    CalcOut(Tree({}), op, Loc())
