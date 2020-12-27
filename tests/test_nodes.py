from boyleworkflow.calc import Loc
from pytest import fixture
from boyleworkflow.nodes import Node


@fixture
def root1():
    return Node({}, "op1", Loc("loc1"))


@fixture
def root2():
    return Node({}, "op2", Loc("loc2"))


@fixture
def derived(root1, root2):
    return Node({Loc("i1"): root1, Loc("i2"): root2}, "op", Loc("out"))


@fixture
def derived_identical(root1, root2):
    return Node({Loc("i2"): root2, Loc("i1"): root1}, "op", Loc("out"))


def test_input_loc_affects_hash(root1):
    op = "op"
    out = Loc("out")
    node_a = Node({Loc("inp_a"): root1}, op, out)
    node_b = Node({Loc("inp_b"): root1}, op, out)
    assert hash(node_a) != hash(node_b)


def test_input_node_affects_hash(root1, root2):
    op = "op"
    out = Loc("out")
    node_a = Node({Loc("inp"): root1}, op, out)
    node_b = Node({Loc("inp"): root2}, op, out)
    assert hash(node_a) != hash(node_b)


def test_op_affects_hash():
    out = Loc("out")
    node_a = Node({}, "op1", out)
    node_b = Node({}, "op2", out)
    assert hash(node_a) != hash(node_b)


def test_out_loc_affects_hash(root1, root2):
    op = "op"
    node_a = Node({}, op, Loc("out1"))
    node_b = Node({}, op, Loc("out2"))
    assert hash(node_a) != hash(node_b)


def test_equality_and_hash_insensitive_to_inp_order(derived, derived_identical):
    assert derived == derived_identical
    assert hash(derived) == hash(derived_identical)


def test_root_has_no_parents(root1):
    assert not root1.parents


def test_derived_has_parents(root1, root2, derived):
    assert derived.parents == {root1, root2}
