from boyleworkflow.calc import Env, Loc, Result, SandboxKey
from dataclasses import dataclass, field
from typing import Collection, Iterable, MutableMapping, Tuple
import pytest
import unittest.mock
from boyleworkflow.make import make
from boyleworkflow.nodes import Node, create_simple_node, create_sibling_nodes

StringFormatOp = Collection[Tuple[Loc, str]]
StringNode = Node[StringFormatOp]


def make_op(**definitions: str) -> StringFormatOp:
    return tuple((Loc(k), v) for k, v in definitions.items())


@pytest.fixture
def hello_node():
    return create_simple_node({}, make_op(out="Hello"), "out")


@pytest.fixture
def hello_world_node(hello_node: StringNode):
    return create_simple_node(
        dict(hello=hello_node), make_op(out="{hello} World"), "out"
    )


@pytest.fixture
def multi_nodes():
    return create_sibling_nodes({}, make_op(a="one", b="two"), ["a", "b"])


@dataclass
class StringFormatEnv:
    output: MutableMapping[Loc, str] = field(default_factory=dict)

    def __post_init__(self):
        self._sandboxes = {}
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
            out_loc: template.format(**sandbox) for out_loc, template in op
        }
        sandbox.update(results)

    def stow(self, sandbox_key: SandboxKey, loc: Loc):
        sandbox = self._sandboxes[sandbox_key]
        value = sandbox[loc]
        digest = Result(f"digest:{value}")
        self._storage[digest] = value
        return digest

    def place(self, sandbox_key: SandboxKey, loc: Loc, digest: Result):
        self._sandboxes[sandbox_key][loc] = self._storage[digest]

    def deliver(self, loc: Loc, digest: Result):
        self.output[loc] = self._storage[digest]


@pytest.fixture
def env():
    return StringFormatEnv()


def test_make_hello(env: StringFormatEnv, hello_node: StringNode):
    make({Loc("hello"): hello_node}, env)
    assert env.output[Loc("hello")] == "Hello"


def test_make_hello_world(env: StringFormatEnv, hello_world_node: StringNode):
    make({Loc("hello_world"): hello_world_node}, env)
    assert env.output[Loc("hello_world")] == "Hello World"


def test_multi_output(env: StringFormatEnv, multi_nodes: Iterable[StringNode]):
    make({n.out: n for n in multi_nodes}, env)
    assert env.output[Loc("a")] == "one"
    assert env.output[Loc("b")] == "two"


def test_multi_output_runs_once(
    env: StringFormatEnv, multi_nodes: Iterable[StringNode]
):
    env = unittest.mock.Mock(wraps=env)
    make({n.out: n for n in multi_nodes}, env)
    env.run_op.assert_called_once()


def test_can_make_one_of_multi(
    env: StringFormatEnv, multi_nodes: Iterable[StringNode]
):
    first, second = multi_nodes
    make({first.out: first}, env)
    assert first.out in env.output
    assert second.out not in env.output
