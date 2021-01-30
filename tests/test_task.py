import pytest
from boyleworkflow.tree import Name
from boyleworkflow.graph import Task

TASK = Task.create({}, "op", ["out"])
TASK_L1 = TASK.descend("level1")
TASK_L2 = TASK_L1.descend("level2")


def test_getitem():
    task = Task.create({}, "op", ["out"])
    (node,) = task.nodes
    assert node == task["out"]


def test_default_not_nested():
    task = Task.create({}, "op", ["out"])
    assert task.levels == ()


def test_descend_does_not_alter():
    task = Task.create({}, "op", ["out"])
    task.descend("level1")
    assert task.levels == ()


def test_descend_creates_level():
    assert TASK.descend("level1").levels == (Name("level1"),)


def test_descend_twice_creates_two_levels():
    assert TASK.descend("l1").descend("l2").levels == (Name("l1"), Name("l2"))


def test_descend_levels_must_be_unique():
    with pytest.raises(ValueError):
        TASK.descend("level1").descend("level1")


def test_ascend_reversed_descend():
    assert TASK_L1.ascend() == TASK
    assert TASK_L2.ascend() == TASK_L1


def test_cannot_ascend_unless_nested():
    with pytest.raises(ValueError):
        TASK.ascend()
