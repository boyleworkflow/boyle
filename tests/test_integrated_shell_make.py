from boyleworkflow.graph import Node
from pathlib import Path
from boyleworkflow.shell import (
    IMPORT_LOC,
    ShellEnv,
    create_import_op,
    create_shell_op,
    ShellRunSystem,
)
from tests.util import create_env_node


def read_str(path: Path) -> str:
    with open(path, "r") as f:
        return f.read()


def write_str(s: str, path: Path):
    with open(path, "w") as f:
        return f.write(s)


def create_import_node(path: str) -> Node:
    return create_env_node({}, create_import_op(path), [IMPORT_LOC])[IMPORT_LOC]


def test_make(tmp_path: Path):
    write_str("Hello ", tmp_path / "hello")
    write_str("Boyle", tmp_path / "boyle")

    hello = create_import_node("hello")
    boyle = create_import_node("boyle")

    result = create_env_node(
        {"greeting": hello, "name": boyle},
        create_shell_op("cat greeting name > greeting_name"),
        ["greeting_name"],
    )

    system = ShellRunSystem(ShellEnv(tmp_path))
    system.make(result)

    assert read_str(system.env.outdir / "greeting_name") == "Hello Boyle"
