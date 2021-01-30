import pytest
from boyleworkflow.tree import Name
from boyleworkflow.nodes import Node, Task


ROOT1 = Node.create({}, "op1", "loc1")
ROOT2 = Node.create({}, "op2", "loc2")
DERIVED = Node.create({"i1": ROOT1, "i2": ROOT2}, "op", "out")
DERIVED_IDENTICAL = Node.create({"i2": ROOT2, "i1": ROOT1}, "op", "out")

TASK = Task.create({}, "op", ["out"])
TASK_L1 = TASK.descend("level1")
TASK_L2 = TASK_L1.descend("level2")


def test_input_loc_affects_hash():
    op = "op"
    out = "out"
    node_a = Node.create({"inp_a": ROOT1}, op, out)
    node_b = Node.create({"inp_b": ROOT1}, op, out)
    assert hash(node_a) != hash(node_b)


def test_input_node_affects_hash():
    op = "op"
    out = "out"
    node_a = Node.create({"inp": ROOT1}, op, out)
    node_b = Node.create({"inp": ROOT2}, op, out)
    assert hash(node_a) != hash(node_b)


def test_op_affects_hash():
    out = "out"
    node_a = Node.create({}, "op1", out)
    node_b = Node.create({}, "op2", out)
    assert hash(node_a) != hash(node_b)


def test_out_loc_affects_hash():
    op = "op"
    node_a = Node.create({}, op, "out1")
    node_b = Node.create({}, op, "out2")
    assert hash(node_a) != hash(node_b)


def test_equality_and_hash_insensitive_to_inp_order():
    assert DERIVED == DERIVED_IDENTICAL
    assert hash(DERIVED) == hash(DERIVED_IDENTICAL)


def test_task_getitem():
    task = Task.create({}, "op", ["out"])
    (node,) = task.nodes
    assert node == task["out"]


def test_root_has_no_parents():
    assert not ROOT1.parents


def test_derived_has_parents():
    assert DERIVED.parents == {ROOT1, ROOT2}


def test_task_default_not_nested():
    task = Task.create({}, "op", ["out"])
    assert task.levels == ()


def test_task_descend_does_not_alter():
    task = Task.create({}, "op", ["out"])
    task.descend("level1")
    assert task.levels == ()


def test_task_descend():
    assert TASK.descend("level1").levels == (Name("level1"),)


def test_task_descend_twice():
    assert TASK.descend("l1").descend("l2").levels == (Name("l1"), Name("l2"))


def test_task_descend_levels_must_be_unique():
    with pytest.raises(ValueError):
        TASK.descend("level1").descend("level1")


def test_task_ascend():
    assert TASK_L1.ascend() == TASK
    assert TASK_L2.ascend() == TASK_L1


def test_task_cannot_ascend_unless_nested():
    with pytest.raises(ValueError):
        TASK.ascend()


def test_node_descend():
    task = TASK
    nested_task = task.descend("level_name")

    node = task["out"]
    nested_node = node.descend("level_name")

    assert nested_task["out"] == nested_node


def test_node_ascend():
    node = TASK["out"]
    assert node.descend("level_name").ascend() == node
