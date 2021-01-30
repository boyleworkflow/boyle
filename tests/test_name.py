import pytest
from boyleworkflow.tree import Name


def test_cannot_be_empty():
    with pytest.raises(ValueError):
        Name("")


def test_cannot_have_slash():
    with pytest.raises(ValueError):
        Name("a/b")
