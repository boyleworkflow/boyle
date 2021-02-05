import pytest
from boyleworkflow.tree import Name, Loc

EMPTY_PATH_STR = "."


def test_from_string():
    loc = Loc.from_string("a/b")
    assert loc.names == (Name("a"), Name("b"))


def test_to_string():
    loc_str = "a/b"
    assert Loc.from_string(loc_str).to_string() == loc_str


def test_empty_loc_from_string():
    assert Loc() == Loc.from_string(EMPTY_PATH_STR)


def test_empty_loc_to_string():
    assert Loc().to_string() == EMPTY_PATH_STR


def test_empty_loc_has_no_names():
    assert Loc().names == ()


def test_from_string_eliminates_leading_dot():
    assert Loc.from_string("./a") == Loc.from_string("a")


def test_from_string_eliminates_trailing_dot():
    assert Loc.from_string("a/.") == Loc.from_string("a")


def test_from_string_eliminates_inner_dots():
    assert Loc.from_string("a/./b/./c") == Loc.from_string("a/b/c")


def test_from_string_eliminates_trailing_slash():
    assert Loc.from_string("a/b/") == Loc.from_string("a/b")


def test_must_be_relative():
    with pytest.raises(ValueError):
        Loc.from_string("/a")


def test_no_loc_from_empty_string():
    with pytest.raises(ValueError):
        Loc.from_string("")


def test_cannot_have_double_slash():
    with pytest.raises(ValueError):
        Loc.from_string("a//b")
