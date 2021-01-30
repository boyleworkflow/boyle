import pytest
from boyleworkflow.tree import Name
from boyleworkflow.graph import Task

TASK_L0 = Task({}, "op", ["out"])
TASK_L1 = TASK_L0.descend("level1")
TASK_L2 = TASK_L1.descend("level2")


def test_getitem():
    task = Task({}, "op", ["out"])
    (node,) = task.nodes
    assert node == task["out"]


def test_default_without_parents_not_nested():
    task = Task({}, "op", ["out"])
    assert task.out_levels == ()


def test_by_default_inherits_parent_levels():
    parent_l1 = TASK_L1
    derived_l1 = Task({"inp": parent_l1["out"]}, "op", ["out"])
    assert derived_l1.out_levels == parent_l1.out_levels


def test_default_can_be_overridden():
    parent_l1 = TASK_L1
    other_levels = (Name("any"), Name("here"))
    derived_l1 = Task({"inp": parent_l1["out"]}, "op", ["out"], out_levels=other_levels)
    assert derived_l1.out_levels == other_levels


def test_descend_does_not_alter():
    task = Task({}, "op", ["out"])
    task.descend("level1")
    assert task.out_levels == ()


def test_descend_creates_level():
    assert TASK_L0.descend("level1").out_levels == (Name("level1"),)


def test_descend_twice_creates_two_levels():
    assert TASK_L0.descend("l1").descend("l2").out_levels == (Name("l1"), Name("l2"))


def test_descend_levels_must_be_unique():
    with pytest.raises(ValueError):
        TASK_L0.descend("level1").descend("level1")


def test_ascend_removes_level():
    assert TASK_L1.ascend().out_levels == TASK_L0.out_levels
    assert TASK_L2.ascend().out_levels == TASK_L1.out_levels


def test_cannot_ascend_unless_nested():
    with pytest.raises(ValueError):
        TASK_L0.ascend()


def test_input_levels_must_match():
    node_l0 = TASK_L0["out"]
    node_l1 = TASK_L1["out"]
    with pytest.raises(ValueError):
        Task({"inp1": node_l0, "inp2": node_l1}, "op", ["out"])
