from boyleworkflow.frozendict import FrozenDict
from tests.util import tree_from_dict
from boyleworkflow.tree import Leaf, Path, Tree, TreeItem
from boyleworkflow.calc import SandboxKey
from dataclasses import dataclass, field
from typing import Dict, Union
import unittest.mock
from boyleworkflow.make import make
from boyleworkflow.nodes import Node, NodeBundle

StringFormatOp = FrozenDict[str, str]


def make_op(**definitions: str) -> StringFormatOp:
    return FrozenDict(definitions)


NestedStrDictItem = Union["NestedStrDict", str]
NestedStrDict = Dict[str, NestedStrDictItem]

PATH_SEP = "/"


def place_nested(mapping: NestedStrDict, path: str, value: NestedStrDictItem):
    *descend_segments, final_segment = path.split(PATH_SEP)
    for segment in descend_segments:
        if segment not in mapping:
            mapping[segment] = {}

        descend_into = mapping[segment]
        if not isinstance(descend_into, dict):
            raise ValueError(f"{segment}: {descend_into}")
        mapping = descend_into

    mapping[final_segment] = value


def pick_nested(mapping: NestedStrDict, path: str) -> NestedStrDictItem:
    *descend_segments, final_segment = path.split(PATH_SEP)
    for segment in descend_segments:
        descend_into = mapping[segment]
        if not isinstance(descend_into, dict):
            raise ValueError(f"{segment}: {descend_into}")
        mapping = descend_into

    return mapping[final_segment]


@dataclass
class StringFormatEnv:
    output: NestedStrDict = field(default_factory=dict)

    def __post_init__(self):
        self._sandboxes: Dict[SandboxKey, NestedStrDict] = {}
        self._storage: Dict[TreeItem, NestedStrDictItem] = {}

    def create_sandbox(self):
        sandbox_key = SandboxKey("sandbox")
        self._sandboxes[sandbox_key] = {}
        return sandbox_key

    def destroy_sandbox(self, sandbox_key: SandboxKey):
        del self._sandboxes[sandbox_key]

    def run_op(self, op: StringFormatOp, sandbox_key: SandboxKey):
        sandbox = self._sandboxes[sandbox_key]
        op_results = {path: template.format(**sandbox) for path, template in op.items()}
        for path, value in op_results.items():
            place_nested(sandbox, path, value)

    def stow(self, sandbox_key: SandboxKey, path: Path):
        sandbox = self._sandboxes[sandbox_key]
        value = pick_nested(sandbox, path.to_string())

        tree_item = Leaf(value) if isinstance(value, str) else tree_from_dict(value)
        self._storage[tree_item] = value
        return tree_item

    def _build_value_from_storage(self, tree_item: TreeItem) -> NestedStrDictItem:
        if isinstance(tree_item, Leaf):
            return self._storage[tree_item]

        return {
            name.value: self._build_value_from_storage(child_item)
            for name, child_item in tree_item.children.items()
        }

    def _place_into(self, tree: Tree, destination: NestedStrDict):
        value = self._build_value_from_storage(tree)
        if not isinstance(value, dict):
            raise ValueError(f"expected a dict but found {repr(value)}")
        destination.update(value)

    def place(self, sandbox_key: SandboxKey, tree: Tree):
        self._place_into(tree, self._sandboxes[sandbox_key])

    def deliver(self, tree: Tree):
        self._place_into(tree, self.output)


HELLO_NODE = Node.create({}, make_op(out="Hello"), "out")
HELLO_WORLD_NODE = Node.create(
    dict(hello=HELLO_NODE), make_op(out="{hello} World"), "out"
)
SIBLING_NODES = NodeBundle.create({}, make_op(a="one", b="two"), ["a", "b"])


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
