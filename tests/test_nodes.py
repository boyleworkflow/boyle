from boyleworkflow.nodes import Node, NodeBundle


ROOT1 = Node.create({}, "op1", "loc1")
ROOT2 = Node.create({}, "op2", "loc2")
DERIVED = Node.create({"i1": ROOT1, "i2": ROOT2}, "op", "out")
DERIVED_IDENTICAL = Node.create({"i2": ROOT2, "i1": ROOT1}, "op", "out")


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
