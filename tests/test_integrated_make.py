from dataclasses import dataclass, field
from typing import Dict, Mapping, Sequence, Union
import unittest.mock
from boyleworkflow.frozendict import FrozenDict
from boyleworkflow.tree import Name, Path, Tree
from boyleworkflow.calc import SandboxKey
from boyleworkflow.make import make
from boyleworkflow.graph import Node, Task

StringFormatOp = FrozenDict[str, str]


def make_op(**definitions: str) -> StringFormatOp:
    return FrozenDict(definitions)


NestedStrDictItem = Union["NestedStrDict", str]
NestedStrDict = Dict[str, NestedStrDictItem]


def place_nested(mapping: NestedStrDict, path: Path, value: NestedStrDictItem):
    *descend_segments, final_segment = [name.value for name in path.names]
    for segment in descend_segments:
        if segment not in mapping:
            mapping[segment] = {}

        descend_into = mapping[segment]
        if not isinstance(descend_into, dict):
            raise ValueError(f"{segment}: {descend_into}")
        mapping = descend_into

    mapping[final_segment] = value


def _pick_nested(
    item: NestedStrDictItem, path_segments: Sequence[Name]
) -> NestedStrDictItem:
    if not path_segments:
        return item

    if not isinstance(item, dict):
        raise ValueError(f"cannot descend to {path_segments} in {repr(item)}")
    first, *rest = path_segments
    return _pick_nested(item[first.value], rest)


def pick_nested(item: NestedStrDictItem, path: Path) -> NestedStrDictItem:
    return _pick_nested(item, path.names)


def build_tree_description(item: NestedStrDictItem) -> Tree:
    if isinstance(item, str):
        return Tree({}, f"leaf:{item}")

    return Tree({Name(k): build_tree_description(v) for k, v in item.items()})


def build_item_from_storage(
    tree: Tree, storage: Mapping[Tree, NestedStrDictItem]
) -> NestedStrDictItem:
    if tree.data:
        return storage[tree]

    return {
        name.value: build_item_from_storage(subtree, storage)
        for name, subtree in tree.items()
    }


@dataclass
class StringFormatEnv:
    output: NestedStrDict = field(default_factory=dict)

    def __post_init__(self):
        self._sandboxes: Dict[SandboxKey, NestedStrDict] = {}
        self._storage: Dict[Tree, NestedStrDictItem] = {}

    def create_sandbox(self):
        sandbox_key = SandboxKey("sandbox")
        self._sandboxes[sandbox_key] = {}
        return sandbox_key

    def destroy_sandbox(self, sandbox_key: SandboxKey):
        del self._sandboxes[sandbox_key]

    def run_op(self, op: StringFormatOp, sandbox_key: SandboxKey):
        sandbox = self._sandboxes[sandbox_key]
        op_results = {
            Path.from_string(path): template.format(**sandbox)
            for path, template in op.items()
        }
        for path, value in op_results.items():
            place_nested(sandbox, path, value)

    def stow(self, sandbox_key: SandboxKey, path: Path):
        sandbox = self._sandboxes[sandbox_key]
        result = pick_nested(sandbox, path)
        tree = build_tree_description(result)
        for path, subtree in tree.walk():
            self._storage[subtree] = pick_nested(result, path)
        return tree

    def _place_into(self, tree: Tree, destination: NestedStrDict):
        item = build_item_from_storage(tree, self._storage)
        if not isinstance(item, dict):
            raise ValueError(f"expected a dict but found {repr(item)}")
        destination.update(item)

    def place(self, sandbox_key: SandboxKey, tree: Tree):
        self._place_into(tree, self._sandboxes[sandbox_key])

    def deliver(self, tree: Tree):
        self._place_into(tree, self.output)


HELLO_NODE = Node.create({}, make_op(out="Hello"), "out")
HELLO_WORLD_NODE = Node.create(
    dict(hello=HELLO_NODE), make_op(out="{hello} World"), "out"
)
SIBLING_NODES = Task({}, make_op(a="one", b="two"), ["a", "b"])


def test_make_hello():
    env = StringFormatEnv()
    make({Path.from_string("hello"): HELLO_NODE}, env)
    assert env.output["hello"] == "Hello"


def test_make_hello_world():
    env = StringFormatEnv()
    make({Path.from_string("hello_world"): HELLO_WORLD_NODE}, env)
    assert env.output["hello_world"] == "Hello World"


def test_multi_output():
    env = StringFormatEnv()
    make({n.out: n for n in SIBLING_NODES.nodes}, env)
    assert env.output["a"] == "one"
    assert env.output["b"] == "two"


def test_multi_output_runs_once():
    env = unittest.mock.Mock(wraps=StringFormatEnv())
    make({n.out: n for n in SIBLING_NODES.nodes}, env)
    env.run_op.assert_called_once()  # type: ignore


def test_can_make_one_of_siblings():
    env = StringFormatEnv()
    first, _ = SIBLING_NODES.nodes
    make({first.out: first}, env)
    assert env.output.keys() == {first.out.to_string()}


def test_can_make_nested():
    env = StringFormatEnv()
    names = Node.create(
        {},
        make_op(
            **{
                "out/first": "Robert",
                "out/last": "Boyle",
            }
        ),
        "out",
    )
    greetings = Node.create(
        {"name": names.descend("name_level")},
        make_op(greeting="Hello {name}!"),
        "greeting",
    )
    make({greetings.out: greetings}, env)
    assert env.output == {
        "greeting": {
            "first": "Hello Robert!",
            "last": "Hello Boyle!",
        }
    }


def test_can_make_twice_nested():
    env = StringFormatEnv()
    names = Node.create(
        {},
        make_op(
            **{
                "out/first": "Robert",
                "out/last": "Boyle",
            }
        ),
        "out",
    )
    greetings = Node.create(
        {"name": names.descend("name_level")},
        make_op(
            **{
                "greetings/English": "Hello {name}!",
                "greetings/Swedish": "Hej {name}!",
            }
        ),
        "greetings",
    ).descend("language")

    make({greetings.out: greetings}, env)
    assert env.output == {
        "greetings": {
            "first": {
                "English": "Hello Robert!",
                "Swedish": "Hej Robert!",
            },
            "last": {
                "English": "Hello Boyle!",
                "Swedish": "Hej Boyle!",
            },
        }
    }


def test_can_nest_node_with_non_nestable_sibling():
    env = StringFormatEnv()

    task_1 = Task(
        {},
        make_op(
            **{
                "to be nested/key 1": "value 1",
                "to be nested/key 2": "value 2",
                "not to be nested": "...",
            }
        ),
        ["to be nested", "not to be nested"],
    )

    parent_node = task_1["to be nested"].descend("nested level")

    derived_node = Node.create(
        {"parent": parent_node},
        make_op(result="{parent.upper()}"),
        "result",
    )

    make({derived_node.out: derived_node}, env)
    assert env.output == {
        "result": {
            "key 1": "VALUE 1",
            "key 2": "VALUE 2",
        }
    }
