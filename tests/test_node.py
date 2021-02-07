import pytest
from boyleworkflow.tree import Name
from boyleworkflow.api import define


def test_node_without_parents_is_not_nested():
    node = define({}, "op", ["out"]).node
    assert not node.inp
    assert node.out_levels == ()


def test_inherits_nested_parent_levels():
    parent_l1 = define({}, "op", "out").split("test")
    derived_l1 = define({"inp": parent_l1}, "op", "out")
    assert len(parent_l1.node.out_levels) == 1
    assert derived_l1.node.out_levels == parent_l1.node.out_levels


def test_split_creates_out_level():
    assert define({}, "op", "out").split("level1").node.out_levels == (Name("level1"),)


def test_split_twice_creates_two_levels():
    node_l2 =  define({}, "op", "out").split("l1").split("l2").node
    assert node_l2.out_levels == (Name("l1"), Name("l2"))


def test_split_levels_must_be_unique():
    with pytest.raises(ValueError):
        define({}, "op", "out").split("l1").split("l1")


def test_input_levels_must_match():
    node_l0 = define({}, "op", "out")
    node_l1 = define({}, "op", "out").split("l1")
    with pytest.raises(ValueError):
        define({"inp1": node_l0, "inp2": node_l1}, "op", ".")
