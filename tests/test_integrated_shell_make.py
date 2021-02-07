from boyleworkflow.storage import describe
from pathlib import Path
from boyleworkflow.shell import (
    IMPORT_LOC,
    create_import_op,
    create_shell_op,
    ShellSystem,
)
from boyleworkflow.api import NodeWrapper, define


def read_str(path: Path) -> str:
    with open(path, "r") as f:
        return f.read()


def write_str(s: str, path: Path):
    with open(path, "w") as f:
        return f.write(s)


def create_import_node(path: str) -> NodeWrapper:
    return define({}, create_import_op(path), [IMPORT_LOC]).pick(str(IMPORT_LOC))


def test_make(tmp_path: Path):
    write_str("Hello ", tmp_path / "hello")
    write_str("Boyle", tmp_path / "boyle")

    hello = create_import_node("hello")
    boyle = create_import_node("boyle")

    result = define(
        {"greeting": hello, "name": boyle},
        create_shell_op("cat greeting name > greeting_name"),
        ["greeting_name"],
    )

    system = ShellSystem(tmp_path)
    system.make(result)

    assert read_str(system.outdir / "greeting_name") == "Hello Boyle"


def test_make_persists_cache_on_disk(tmp_path: Path):
    # If this runs twice it will produce different outputs
    node = define(
        {},
        create_shell_op("""python -c "import uuid; print(uuid.uuid4())" > outfile"""),
        ["outfile"],
    )

    system_1 = ShellSystem(tmp_path)
    system_1.make(node)
    result_1 = describe(system_1.outdir)

    system_2 = ShellSystem(tmp_path)
    system_2.make(node)
    result_2 = describe(system_2.outdir)

    assert result_1 == result_2
