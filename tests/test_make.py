from boyleworkflow.tree import Leaf, Path, Tree
from boyleworkflow.calc import SandboxKey
from dataclasses import dataclass, field
from typing import Dict, Iterable, MutableMapping, Tuple
import unittest.mock
from boyleworkflow.make import make
from boyleworkflow.nodes import Node, NodeBundle

StringFormatOp = Tuple[Tuple[Path, str], ...]


def make_op(**definitions: str) -> StringFormatOp:
    return tuple((Path.from_string(k), v) for k, v in definitions.items())


HELLO_NODE = Node.create({}, make_op(out="Hello"), "out")
HELLO_WORLD_NODE = Node.create(
    dict(hello=HELLO_NODE), make_op(out="{hello} World"), "out"
)
SIBLING_NODES = NodeBundle.create({}, make_op(a="one", b="two"), ["a", "b"])


@dataclass
class StringFormatEnv:
    output: MutableMapping[str, str] = field(default_factory=dict)

    def __post_init__(self):
        self._sandboxes: Dict[SandboxKey, Dict[str, str]] = {}
        self._storage = {}

    def create_sandbox(self):
        sandbox_key = SandboxKey("sandbox")
        self._sandboxes[sandbox_key] = {}
        return sandbox_key

    def destroy_sandbox(self, sandbox_key: SandboxKey):
        del self._sandboxes[sandbox_key]

    def run_op(self, op: StringFormatOp, sandbox_key: SandboxKey):
        sandbox = self._sandboxes[sandbox_key]
        results = {
            path.to_string(): template.format(**sandbox) for path, template in op
        }
        sandbox.update(results)

    def stow(self, sandbox_key: SandboxKey, path: Path):
        sandbox = self._sandboxes[sandbox_key]
        value = sandbox[path.to_string()]
        leaf = Leaf(f"leaf:{value}")
        self._storage[leaf] = value
        return leaf

    def _place_into(self, tree: Tree, destination: MutableMapping[str, str]):
        for path, item in tree.walk():
            if isinstance(item, Leaf):
                destination[path.to_string()] = self._storage[item]

    def place(self, sandbox_key: SandboxKey, tree: Tree):
        self._place_into(tree, self._sandboxes[sandbox_key])

    def deliver(self, tree: Tree):
        self._place_into(tree, self.output)


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
