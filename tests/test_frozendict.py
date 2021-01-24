from typing import Dict, Mapping
from boyleworkflow.frozendict import FrozenDict


def test_eq_not_order_dependent():
    fd1 = FrozenDict({"a": 1, "b": 2})
    fd2 = FrozenDict({"b": 2, "a": 1})
    assert fd1 == fd2


def test_hash_not_order_dependent():
    fd1 = FrozenDict({"a": 1, "b": 2})
    fd2 = FrozenDict({"b": 2, "a": 1})
    assert hash(fd1) == hash(fd2)


def test_len_0():
    assert len(FrozenDict({})) == 0


def test_len_not_0():
    assert len(FrozenDict({"a": 1, "b": 2, "c": 3})) == 3


def test_iter():
    fd = FrozenDict({"a": 1, "b": 2, "c": 3})
    assert tuple(iter(fd)) == ("a", "b", "c")


def test_items():
    d = {"a": 1, "b": 2, "c": 3}
    fd = FrozenDict(d)
    assert tuple(fd.items()) == tuple(d.items())


def test_get_existing():
    fd = FrozenDict({"a": 1, "b": 2, "c": 3})
    assert fd.get("b", 55) == 2


def test_get_default():
    fd = FrozenDict({})
    assert fd.get("key", 123) == 123


def test_keys():
    d = {"a": 1, "b": 2, "c": 3}
    fd = FrozenDict(d)
    assert tuple(fd.keys()) == tuple(d.keys())


def test_values():
    d = {"a": 1, "b": 2, "c": 3}
    fd = FrozenDict(d)
    assert tuple(fd.values()) == tuple(d.values())
