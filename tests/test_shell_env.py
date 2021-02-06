import pytest
from boyleworkflow.loc import Loc
from pathlib import Path
from boyleworkflow.shell import ShellEnv, IMPORT_LOC, create_shell_op, create_import_op
from boyleworkflow.storage import describe
from tests.test_storage import create_spec_tree


def test_root_dir_must_be_absolute():
    with pytest.raises(ValueError):
        ShellEnv(Path("some/relative/path"))


def test_root_dir_must_exist(tmp_path: Path):
    with pytest.raises(ValueError):
        ShellEnv(tmp_path / "nonexistent")


def test_root_dir_must_be_directory(tmp_path: Path):
    a_path = tmp_path / "anything"
    a_path.touch()
    with pytest.raises(ValueError):
        ShellEnv(a_path)


def test_can_place_and_stow(tmp_path: Path):
    inp_tree = create_spec_tree({"has": {"nested": {"subdirs": {}}}})
    env = ShellEnv(tmp_path)
    sandbox = env.create_sandbox()
    env.place(sandbox, inp_tree)
    stowed = env.stow(sandbox, Loc("has/nested"))
    assert stowed == create_spec_tree({"subdirs": {}})


def test_can_deliver(tmp_path: Path):
    tree = create_spec_tree({"an empty dir": {}})
    env = ShellEnv(tmp_path)
    env.deliver(tree)
    assert describe(env.outdir) == tree


def test_can_run_shell_cmd_op(tmp_path: Path):
    op = create_shell_op("echo test > a_file.txt")
    env = ShellEnv(tmp_path)
    sandbox = env.create_sandbox()
    env.run_op(op, sandbox)
    tree = env.stow(sandbox, Loc("."))
    env.deliver(tree)
    with open(env.outdir / "a_file.txt", "r") as f:
        assert f.read() == "test\n"


def test_can_run_import_op_on_file(tmp_path: Path):
    input_path = tmp_path / "input_file.txt"
    with open(input_path, "w") as f:
        f.write("test")

    op = create_import_op("input_file.txt")

    env = ShellEnv(tmp_path)
    sandbox = env.create_sandbox()
    env.run_op(op, sandbox)
    tree = env.stow(sandbox, IMPORT_LOC)
    assert describe(input_path) == tree


def test_can_run_import_op_on_dir(tmp_path: Path):
    input_dir = tmp_path / "input_dir"
    input_dir.mkdir()
    with open(input_dir / "some_file", "w") as f:
        f.write("test")

    op = create_import_op("input_dir")

    env = ShellEnv(tmp_path)
    sandbox = env.create_sandbox()
    env.run_op(op, sandbox)
    tree = env.stow(sandbox, IMPORT_LOC)
    assert describe(input_dir) == tree
