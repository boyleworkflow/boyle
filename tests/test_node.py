import pytest
from boyleworkflow.tree import Name
from tests.util import create_env_node

NODE_L0 = create_env_node({}, "op", ["out"])
NODE_L2 = NODE_L0.split("level1")
NODE_L3 = NODE_L2.split("level2")


def test_env_node_without_parents_is_not_nested():
    node = create_env_node({}, "op", ["out"])
    assert node.out_levels == ()


def test_depth_equals_number_of_levels():
    assert NODE_L0.depth == 0
    assert NODE_L2.depth == 1
    assert NODE_L3.depth == 2


def test_inherits_parent_levels():
    parent_l1 = NODE_L2
    derived_l1 = create_env_node({"inp": parent_l1}, "op", ["out"])
    assert derived_l1.out_levels == parent_l1.out_levels


def test_split_creates_out_level():
    assert NODE_L0.split("level1").out_levels == (Name("level1"),)


def test_split_twice_creates_two_levels():
    assert NODE_L0.split("l1").split("l2").out_levels == (Name("l1"), Name("l2"))


def test_split_levels_must_be_unique():
    with pytest.raises(ValueError):
        NODE_L0.split("level1").split("level1")


def test_input_levels_must_match():
    node_l0 = NODE_L0
    node_l1 = NODE_L2
    with pytest.raises(ValueError):
        create_env_node({"inp1": node_l0, "inp2": node_l1}, "op", ["inp1", "inp2"])
