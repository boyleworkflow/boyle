from boyleworkflow.log import Log
from boyleworkflow.graph import Node
from dataclasses import dataclass
from typing import Dict, List, Mapping, Optional, Sequence, Union
from boyleworkflow.frozendict import FrozenDict
from boyleworkflow.tree import Name, Path, Tree
from boyleworkflow.calc import SandboxKey
import boyleworkflow.scheduling
from boyleworkflow.runcalc import RunSystem
import tests.util

StringFormatOp = FrozenDict[str, str]


def create_log() -> Log:
    return Log()


def create_env_node(inp: Mapping[str, Node], op: Mapping[str, str], out: List[str]):
    return tests.util.create_env_node(inp, FrozenDict(op), out)


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
    output: Optional[NestedStrDictItem] = None
    op_run_count: int = 0

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
        self.op_run_count += 1

    def stow(self, sandbox_key: SandboxKey, path: Path):
        sandbox = self._sandboxes[sandbox_key]
        result = pick_nested(sandbox, path)
        tree = build_tree_description(result)
        for path, subtree in tree.walk():
            self._storage[subtree] = pick_nested(result, path)
        return tree

    def place(self, sandbox_key: SandboxKey, tree: Tree):
        item = build_item_from_storage(tree, self._storage)
        if not isinstance(item, dict):
            raise ValueError(f"cannot place non-dict item {item} in sandbox")
        self._sandboxes[sandbox_key] = item

    def deliver(self, tree: Tree):
        self.output = build_item_from_storage(tree, self._storage)


@dataclass
class StringFormatRunSystem(RunSystem):
    env: StringFormatEnv

    def make(self, node: Node):
        results = boyleworkflow.scheduling.make({node}, self)
        self.env.deliver(results[node])

    @property
    def output(self):
        return self.env.output


def create_run_system_without_cache():
    return StringFormatRunSystem(StringFormatEnv())


def test_make_hello():
    system = create_run_system_without_cache()
    hello_node = create_env_node({}, {"hello": "Hello"}, ["hello"])
    system.make(hello_node)
    assert system.output == {"hello": "Hello"}


def test_make_hello_world():
    system = create_run_system_without_cache()
    hello_node = create_env_node({}, {"hello": "Hello"}, ["hello"])
    hello_world_node = create_env_node(
        {".": hello_node},
        {"hello_world": "{hello} World"},
        ["hello_world"],
    )
    system.make(hello_world_node)
    assert system.output == {"hello_world": "Hello World"}


def test_nest():
    system = create_run_system_without_cache()
    hello_node = create_env_node({}, {"hello": "Hello"}, ["hello"])
    system.make(hello_node.nest("greeting"))
    assert system.output == {"greeting": {"hello": "Hello"}}


def test_pick():
    system = create_run_system_without_cache()
    hello_node = create_env_node({}, {"hello": "Hello"}, ["hello"])
    system.make(hello_node["hello"])
    assert system.output == "Hello"


def test_merge():
    system = create_run_system_without_cache()
    node_1 = create_env_node({}, {"first": "Robert"}, ["first"])
    node_2 = create_env_node({}, {"last": "Boyle"}, ["last"])
    merged = node_1.merge(node_2)
    system.make(merged)
    assert system.output == {"first": "Robert", "last": "Boyle"}


def test_multi_output():
    system = create_run_system_without_cache()
    multi_output_node = create_env_node({}, {"a": "one", "b": "two"}, ["a", "b"])
    system.make(multi_output_node)
    assert system.output == {"a": "one", "b": "two"}


def test_separated_and_recombined_siblings_runs_only_once():
    system = create_run_system_without_cache()
    multi_output_node = create_env_node({}, {"a": "one", "b": "two"}, ["a", "b"])
    a = multi_output_node["a"]
    b = multi_output_node["b"]
    combined = a.nest("A").merge(b.nest("B"))
    system.make(combined)
    assert system.env.op_run_count == 1


def test_can_split():
    system = create_run_system_without_cache()
    names = (
        create_env_node(
            {},
            {
                "out/first": "Robert",
                "out/last": "Boyle",
            },
            ["out"],
        )
        .pick("out")
        .split("name_level")
    )
    system.make(names)
    assert system.output == {
        "first": "Robert",
        "last": "Boyle",
    }


def test_can_map_on_nested_level_1():
    system = create_run_system_without_cache()
    names = (
        create_env_node(
            {},
            {
                "out/first": "Robert",
                "out/last": "Boyle",
            },
            ["out"],
        )
        .pick("out")
        .split("name_level")
    )

    greetings = create_env_node(
        {"name": names},
        {"greeting": "Hello {name}!"},
        ["greeting"],
    )["greeting"]
    system.make(greetings)
    assert system.output == {
        "first": "Hello Robert!",
        "last": "Hello Boyle!",
    }


def test_can_map_on_nested_level_2():
    system = create_run_system_without_cache()

    names = (
        create_env_node(
            {},
            {
                "out/first": "Robert",
                "out/last": "Boyle",
            },
            ["out"],
        )
        .pick("out")
        .split("name part")
    )

    greetings = (
        create_env_node(
            {"name": names},
            {
                "greetings/English": "Hello {name}!",
                "greetings/Swedish": "Hej {name}!",
            },
            ["greetings"],
        )
        .pick("greetings")
        .split("language")
    )

    system.make(greetings)
    assert system.output == {
        "first": {
            "English": "Hello Robert!",
            "Swedish": "Hej Robert!",
        },
        "last": {
            "English": "Hello Boyle!",
            "Swedish": "Hej Boyle!",
        },
    }


def test_can_nest_node_with_non_nestable_sibling():
    system = create_run_system_without_cache()

    root_node = create_env_node(
        {},
        {
            "to be nested/key 1": "1",
            "to be nested/key 2": "2",
            "not to be nested": "...",
        },
        ["to be nested", "not to be nested"],
    )

    parent_node = root_node["to be nested"].split("level name")

    derived_node = create_env_node(
        {"parent": parent_node},
        {"result": "{parent} {parent}"},
        ["result"],
    ).pick("result")

    system.make(derived_node)
    assert system.output == {
        "key 1": "1 1",
        "key 2": "2 2",
    }
