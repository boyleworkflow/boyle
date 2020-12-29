from boyleworkflow.calc import Loc
from dataclasses import dataclass, field
from typing import MutableMapping, Sequence, Tuple
import pytest
import unittest.mock
from boyleworkflow.make import make
from boyleworkflow.nodes import create_simple_node, create_sibling_nodes


def make_op(**definitions: str) -> Sequence[Tuple[Loc, str]]:
    return tuple((Loc(k), v) for k, v in definitions.items())


@pytest.fixture
def hello_node():
    return create_simple_node({}, make_op(out="Hello"), "out")


@pytest.fixture
def hello_world_node(hello_node):
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
        sandbox_key = "sandbox"
        self._sandboxes[sandbox_key] = {}
        return sandbox_key

    def destroy_sandbox(self, sandbox_key):
        del self._sandboxes[sandbox_key]

    def run_op(self, op, sandbox_key):
        sandbox = self._sandboxes[sandbox_key]
        results = {
            out_loc: template.format(**sandbox) for out_loc, template in op
        }
        sandbox.update(results)

    def stow(self, sandbox_key, loc):
        sandbox = self._sandboxes[sandbox_key]
        value = sandbox[loc]
        digest = f"digest:{value}"
        self._storage[digest] = value
        return digest

    def place(self, sandbox_key, loc, digest):
        self._sandboxes[sandbox_key][loc] = self._storage[digest]

    def deliver(self, loc, digest):
        self.output[loc] = self._storage[digest]


@pytest.fixture
def string_format_env():
    return StringFormatEnv()


def test_make_hello(string_format_env, hello_node):
    make({Loc("hello"): hello_node}, string_format_env)
    assert string_format_env.output["hello"] == "Hello"


def test_make_hello_world(string_format_env, hello_world_node):
    make({Loc("hello_world"): hello_world_node}, string_format_env)
    assert string_format_env.output["hello_world"] == "Hello World"


def test_multi_output(string_format_env, multi_nodes):
    make({n.out: n for n in multi_nodes}, string_format_env)
    assert string_format_env.output["a"] == "one"
    assert string_format_env.output["b"] == "two"


def test_multi_output_runs_once(string_format_env, multi_nodes):
    env = unittest.mock.Mock(wraps=string_format_env)
    make({n.out: n for n in multi_nodes}, env)
    env.run_op.assert_called_once()


def test_can_make_one_of_multi(string_format_env, multi_nodes):
    first, second = multi_nodes
    make({first.out: first}, string_format_env)
    assert first.out in string_format_env.output
    assert second.out not in string_format_env.output
