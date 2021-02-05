import pytest
from boyleworkflow.loc import Name, Loc

EMPTY_PATH_STR = "."


def test_from_string():
    loc = Loc("a/b")
    assert loc.names == (Name("a"), Name("b"))


def test_to_string():
    loc_str = "a/b"
    assert str(Loc(loc_str)) == loc_str


def test_empty_loc_from_string():
    assert Loc() == Loc(EMPTY_PATH_STR)


def test_empty_loc_to_string():
    assert str(Loc()) == EMPTY_PATH_STR


def test_empty_loc_has_no_names():
    assert Loc().names == ()


def test_from_string_eliminates_leading_dot():
    assert Loc("./a") == Loc("a")


def test_from_string_eliminates_trailing_dot():
    assert Loc("a/.") == Loc("a")


def test_from_string_eliminates_inner_dots():
    assert Loc("a/./b/./c") == Loc("a/b/c")


def test_from_string_eliminates_trailing_slash():
    assert Loc("a/b/") == Loc("a/b")


def test_must_be_relative():
    with pytest.raises(ValueError):
        Loc("/a")


def test_no_loc_from_empty_string():
    with pytest.raises(ValueError):
        Loc("")


def test_cannot_have_double_slash():
    with pytest.raises(ValueError):
        Loc("a//b")
