from boyleworkflow.nodes import Node, NodeBundle
from pytest import fixture


@fixture
def root1():
    return Node.create({}, "op1", "loc1")


@fixture
def root2():
    return Node.create({}, "op2", "loc2")


@fixture
def derived(root1, root2):
    return Node.create({"i1": root1, "i2": root2}, "op", "out")


@fixture
def derived_identical(root1, root2):
    return Node.create({"i2": root2, "i1": root1}, "op", "out")


def test_input_loc_affects_hash(root1):
    op = "op"
    out = "out"
    node_a = Node.create({"inp_a": root1}, op, out)
    node_b = Node.create({"inp_b": root1}, op, out)
    assert hash(node_a) != hash(node_b)


def test_input_node_affects_hash(root1, root2):
    op = "op"
    out = "out"
    node_a = Node.create({"inp": root1}, op, out)
    node_b = Node.create({"inp": root2}, op, out)
    assert hash(node_a) != hash(node_b)


def test_op_affects_hash():
    out = "out"
    node_a = Node.create({}, "op1", out)
    node_b = Node.create({}, "op2", out)
    assert hash(node_a) != hash(node_b)


def test_out_loc_affects_hash(root1, root2):
    op = "op"
    node_a = Node.create({}, op, "out1")
    node_b = Node.create({}, op, "out2")
    assert hash(node_a) != hash(node_b)


def test_equality_and_hash_insensitive_to_inp_order(derived, derived_identical):
    assert derived == derived_identical
    assert hash(derived) == hash(derived_identical)


def test_bundle_getitem():
    bundle = NodeBundle.create({}, "op", ["out"])
    (node,) = bundle.nodes
    assert node == bundle["out"]


def test_root_has_no_parents(root1):
    assert not root1.parents


def test_derived_has_parents(root1, root2, derived):
    assert derived.parents == {root1, root2}
