import pytest
from boyleworkflow.tree import Name, Path

EMPTY_PATH_STR = "."


def test_from_string():
    path = Path.from_string("a/b")
    assert path.names == (Name("a"), Name("b"))


def test_to_string():
    path_str = "a/b"
    assert Path.from_string(path_str).to_string() == path_str


def test_empty_path_from_string():
    assert Path() == Path.from_string(EMPTY_PATH_STR)


def test_empty_path_to_string():
    assert Path().to_string() == EMPTY_PATH_STR


def test_empty_path_has_no_names():
    assert Path().names == ()


def test_from_string_eliminates_leading_dot():
    assert Path.from_string("./a") == Path.from_string("a")


def test_from_string_eliminates_trailing_dot():
    assert Path.from_string("a/.") == Path.from_string("a")


def test_from_string_eliminates_inner_dots():
    assert Path.from_string("a/./b/./c") == Path.from_string("a/b/c")


def test_from_string_eliminates_trailing_slash():
    assert Path.from_string("a/b/") == Path.from_string("a/b")


def test_must_be_relative():
    with pytest.raises(ValueError):
        Path.from_string("/a")


def test_no_path_from_empty_string():
    with pytest.raises(ValueError):
        Path.from_string("")


def test_cannot_have_double_slash():
    with pytest.raises(ValueError):
        Path.from_string("a//b")
