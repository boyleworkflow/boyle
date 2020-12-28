from boyleworkflow.calc import Loc
from dataclasses import dataclass, field
from typing import MutableMapping
import pytest
from boyleworkflow.make import make
from boyleworkflow.nodes import Node

out_loc = Loc("out")


@pytest.fixture
def hello_node():
    return Node({}, (out_loc, "Hello"), out_loc)


@pytest.fixture
def hello_world_node(hello_node):
    return Node({Loc("hello"): hello_node}, (out_loc, "{hello} World"), out_loc)


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
        out_loc, template = op
        sandbox[out_loc] = template.format(**sandbox)

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
