from typing import Mapping, Union
import pytest
from boyleworkflow.tree import Tree, Leaf, TreeCollision, Path, Name

StrTreeItem = Union["StrTree", str]
StrTree = Mapping[str, StrTreeItem]


def _create_tree_item(value: StrTreeItem):
    if isinstance(value, str):
        return Leaf(value)
    else:
        return tree_from_dict(value)


def tree_from_dict(d: StrTree) -> Tree:
    return Tree({Name(k): _create_tree_item(v) for k, v in d.items()})


def test_name_cannot_be_empty():
    with pytest.raises(ValueError):
        Name("")


def test_name_cannot_have_slash():
    with pytest.raises(ValueError):
        Name("a/b")


def test_name_cannot_have_slash():
    with pytest.raises(ValueError):
        Name("a/b")


def test_path_from_string():
    path = Path.from_string("a/b")
    assert path.names == (Name("a"), Name("b"))


def test_path_must_be_relative():
    with pytest.raises(ValueError):
        Path.from_string("/a")


def test_empty_path_has_no_names():
    assert Path().names == ()


def test_empty_path_from_string():
    assert Path() == Path.from_string(".")


def test_no_path_from_empty_string():
    with pytest.raises(ValueError):
        Path.from_string("")


def test_path_from_string_eliminates_leading_dot():
    assert Path.from_string("./a") == Path.from_string("a")


def test_path_from_string_eliminates_trailing_dot():
    assert Path.from_string("a/.") == Path.from_string("a")


def test_path_from_string_eliminates_inner_dots():
    assert Path.from_string("a/./b/./c") == Path.from_string("a/b/c")


def test_path_from_string_eliminates_trailing_slash():
    assert Path.from_string("a/b/") == Path.from_string("a/b")


def test_path_cannot_have_double_slash():
    with pytest.raises(ValueError):
        Path.from_string("a//b")


def test_from_nested_item():
    result = Tree.from_nested_items({Path.from_string("a/b/c"): Leaf("x")})
    expected_result = tree_from_dict(
        {
            "a": {
                "b": {
                    "c": "x",
                },
            }
        }
    )
    assert result == expected_result


def test_merge_disjoint():
    tree_1 = tree_from_dict(
        {
            "a": {
                "b": "x",
            }
        }
    )
    tree_2 = tree_from_dict(
        {
            "c": "y",
        }
    )
    combined = tree_from_dict(
        {
            "a": {
                "b": "x",
            },
            "c": "y",
        }
    )
    merged = tree_1.merge(tree_2)
    assert merged == combined


def test_merge_inside():
    tree_1 = tree_from_dict(
        {
            "a": {
                "b1": "x",
            }
        }
    )
    tree_2 = tree_from_dict(
        {
            "a": {
                "b2": "x",
            }
        }
    )
    combined = tree_from_dict(
        {
            "a": {
                "b1": "x",
                "b2": "x",
            }
        }
    )
    merged = tree_1.merge(tree_2)
    assert merged == combined


def test_merge_collision_leaf_leaf():
    tree_1 = tree_from_dict({"a": "x"})
    tree_2 = tree_from_dict({"a": "y"})
    with pytest.raises(TreeCollision):
        tree_1.merge(tree_2)


def test_merge_collision_leaf_subtree():
    tree_1 = tree_from_dict({"a": "x"})
    tree_2 = tree_from_dict({"a": {}})
    with pytest.raises(TreeCollision):
        tree_1.merge(tree_2)


def test_merge_collision_nested():
    tree_1 = tree_from_dict({"a": {"b": "x"}})
    tree_2 = tree_from_dict({"a": {"b": "y"}})
    with pytest.raises(TreeCollision):
        tree_1.merge(tree_2)


def test_merge_identical_leaf_no_collision():
    tree_1 = tree_from_dict({"a": "x"})
    tree_2 = tree_from_dict({"a": "x"})
    merged = tree_1.merge(tree_2)
    assert merged == tree_1
    assert merged == tree_2


def test_merge_identical_tree_no_collision():
    tree_1 = tree_from_dict({"a": {"b": "x"}})
    tree_2 = tree_from_dict({"a": {"b": "x"}})
    merged = tree_1.merge(tree_2)
    assert merged == tree_1
    assert merged == tree_2


def test_walk():
    tree = tree_from_dict(
        {
            "a": {"b": "x"},
            "c": "y",
        }
    )
    items = {
        Path.from_string("a"): tree[Name("a")],
        Path.from_string("a/b"): Leaf("x"),
        Path.from_string("c"): Leaf("y"),
    }

    assert items == dict(tree.walk())
