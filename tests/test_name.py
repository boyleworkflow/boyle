import pytest
from boyleworkflow.tree import Name


def test_cannot_be_empty():
    with pytest.raises(ValueError):
        Name("")

def test_cannot_be_dot():
    with pytest.raises(ValueError):
        Name(".")

def test_cannot_be_double_dot():
    with pytest.raises(ValueError):
        Name("..")


def test_cannot_have_slash():
    with pytest.raises(ValueError):
        Name("a/b")

def test_to_str():
    assert str(Name("asdf 123")) == "asdf 123"
