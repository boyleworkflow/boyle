import pytest
from boyleworkflow.tree import Name
from boyleworkflow.nodes import Node, NodeBundle


ROOT1 = Node.create({}, "op1", "loc1")
ROOT2 = Node.create({}, "op2", "loc2")
DERIVED = Node.create({"i1": ROOT1, "i2": ROOT2}, "op", "out")
DERIVED_IDENTICAL = Node.create({"i2": ROOT2, "i1": ROOT1}, "op", "out")

BUNDLE = NodeBundle.create({}, "op", ["out"])
BUNDLE_L1 = BUNDLE.descend("level1")
BUNDLE_L2 = BUNDLE_L1.descend("level2")


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


def test_bundle_getitem():
    bundle = NodeBundle.create({}, "op", ["out"])
    (node,) = bundle.nodes
    assert node == bundle["out"]


def test_root_has_no_parents():
    assert not ROOT1.parents


def test_derived_has_parents():
    assert DERIVED.parents == {ROOT1, ROOT2}


def test_node_bundle_default_not_nested():
    bundle = NodeBundle.create({}, "op", ["out"])
    assert bundle.levels == ()


def test_node_bundle_descend_does_not_alter():
    bundle = NodeBundle.create({}, "op", ["out"])
    bundle.descend("level1")
    assert bundle.levels == ()


def test_node_bundle_descend():
    assert BUNDLE.descend("level1").levels == (Name("level1"),)


def test_node_bundle_descend_twice():
    assert BUNDLE.descend("l1").descend("l2").levels == (Name("l1"), Name("l2"))


def test_node_bundle_descend_levels_must_be_unique():
    with pytest.raises(ValueError):
        BUNDLE.descend("level1").descend("level1")


def test_node_bundle_ascend():
    assert BUNDLE_L1.ascend() == BUNDLE
    assert BUNDLE_L2.ascend() == BUNDLE_L1


def test_node_bundle_cannot_ascend_unless_nested():
    with pytest.raises(ValueError):
        BUNDLE.ascend()


def test_node_descend():
    bundle = BUNDLE
    nested_bundle = bundle.descend("level_name")

    node = bundle["out"]
    nested_node = node.descend("level_name")

    assert nested_bundle["out"] == nested_node


def test_node_ascend():
    node = BUNDLE["out"]
    assert node.descend("level_name").ascend() == node
