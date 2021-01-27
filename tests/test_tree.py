import pytest
from boyleworkflow.tree import Tree, TreeCollision, Path, Name
from tests.util import tree_from_dict

EMPTY_PATH_STR = "."


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


def test_path_to_string():
    path_str = "a/b"
    assert Path.from_string(path_str).to_string() == path_str


def test_empty_path_to_string():
    assert Path().to_string() == EMPTY_PATH_STR


def test_path_must_be_relative():
    with pytest.raises(ValueError):
        Path.from_string("/a")


def test_empty_path_has_no_names():
    assert Path().names == ()


def test_empty_path_from_string():
    assert Path() == Path.from_string(EMPTY_PATH_STR)


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
    result = Tree.from_nested_items({Path.from_string("a/b/c"): Tree({}, "x")})
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


def test_from_nested_item():
    empty_tree = Tree({})
    result = Tree.from_nested_items({})
    assert result == empty_tree


def test_tree_eq_independent_of_order():
    tree_1 = Tree({Name("a1"): Tree({}, "b"), Name("a2"): Tree({})})
    tree_2 = Tree({Name("a2"): Tree({}), Name("a1"): Tree({}, "b")})
    assert tree_1 == tree_2


def test_tree_hash_independent_of_order():
    tree_1 = Tree({Name("a1"): Tree({}, "b"), Name("a2"): Tree({})})
    tree_2 = Tree({Name("a2"): Tree({}), Name("a1"): Tree({}, "b")})
    assert hash(tree_1) == hash(tree_2)


def tree_getitem():
    tree = tree_from_dict({"a": {"b": "x"}})
    subtree = tree_from_dict({"b": "x"})
    assert tree[Name("a")] == subtree


def tree_iter():
    tree = tree_from_dict(
        {
            "a": {},
            "b": "x",
        }
    )
    assert list(iter(tree)) == [Name("a"), Name("b")]


def tree_len():
    tree = tree_from_dict(
        {
            "a": {},
            "b": "x",
        }
    )
    assert len(tree) == 2


def test_tree_pick():
    tree = tree_from_dict({"a": {"b": "x"}})
    path = Path.from_string("a/b")
    assert tree.pick(path) == Tree({}, "x")


def test_tree_pick_empty_path():
    tree = tree_from_dict({"a": {"b": "x"}})
    path = Path.from_string(".")
    assert tree.pick(path) == tree


def test_tree_no_picking_too_deep():
    tree = tree_from_dict({"a": {"b": "x"}})
    path = Path.from_string("a/b/x/z")
    with pytest.raises(ValueError):
        tree.pick(path)


def test_tree_no_picking_unavailable():
    tree = tree_from_dict({"a": {"b": "x"}})
    path = Path.from_string("a/c")
    with pytest.raises(ValueError):
        tree.pick(path)


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
    all_paths = [Path.from_string(s) for s in [".", "a", "a/b", "c"]]
    items = {path: tree.pick(path) for path in all_paths}

    assert items == dict(tree.walk())


def test_iter_empty_tree_level_0():
    tree = tree_from_dict({})
    expected_result = {Path(()): tree}
    result = dict(tree.iter_level(0))
    assert result == expected_result


def test_iter_non_empty_tree_level_0():
    tree = tree_from_dict(
        {
            "a1": "b",
            "a2": {"b": "c"},
        }
    )
    expected_result = {Path(()): tree}
    result = dict(tree.iter_level(0))
    assert result == expected_result


def test_iter_non_empty_tree_level_1():
    tree = tree_from_dict(
        {
            "a1": "b",
            "a2": {"b": "c"},
        }
    )
    level_1_paths = [Path.from_string(s) for s in ["a1", "a2"]]
    expected_result = {path: tree.pick(path) for path in level_1_paths}
    result = dict(tree.iter_level(1))
    assert result == expected_result


def test_iter_tree_level_2():
    tree = tree_from_dict(
        {
            "a1": {
                "b1": {
                    "c1": "111",
                    "c2": "112",
                },
                "b2": {
                    "c1": "121",
                    "c2": "122",
                },
            },
            "a2": {
                "b1": {
                    "c1": {"d1": "2111"},
                },
            },
            "a3": {
                "b1": {},
            },
        }
    )

    level_2_paths = [Path.from_string(s) for s in ["a1/b1", "a1/b2", "a2/b1", "a3/b1"]]
    expected_result = {path: tree.pick(path) for path in level_2_paths}
    result = dict(tree.iter_level(2))
    assert result == expected_result


def test_cannot_iter_trees_beyond_min_depth():
    tree = tree_from_dict(
        {
            "a1": {"b": "c"},
            "a2": "b",
        }
    )

    list(tree.iter_level(1))  # level a works
    with pytest.raises(ValueError):
        list(tree.iter_level(2))  # level b does not work


def test_nest_at_root():
    tree = tree_from_dict({"a": "b"})
    assert tree.nest(Path(())) == tree


def test_nest_deep_below():
    tree = tree_from_dict({"c": "d"})
    path = Path.from_string("a/b")
    assert tree.nest(path).pick(path) == tree


def _convert_to_upper_case(tree: Tree) -> Tree:
    return Tree(
        {Name(name.value.upper()): subtree for name, subtree in tree.items()},
        tree.data.upper(),  # type: ignore
    )


def test_tree_items():
    tree = tree_from_dict({"a1": {"b": "c"}, "a2": {}})
    assert dict(tree.items()) == {name: tree[name] for name in map(Name, ["a1", "a2"])}
