from __future__ import annotations
import subprocess
import shutil
from typing import Mapping, cast
from uuid import uuid4
from boyleworkflow.loc import Loc
from dataclasses import dataclass, field
from pathlib import Path
from boyleworkflow.tree import Tree
from boyleworkflow.calc import Op, SandboxKey
from boyleworkflow.storage import Storage, loc_to_rel_path, describe
from boyleworkflow.frozendict import FrozenDict
from boyleworkflow.graph import Node
import boyleworkflow.scheduling
from boyleworkflow.runcalc import NodeRunner


BOYLE_DIR = ".boyle"
DEFAULT_OUTDIR = Path("output")
IMPORT_LOC = Loc("imported")
_STORAGE_SUBDIR = "storage"


def create_import_op(path: str) -> Op:
    return FrozenDict({"op_type": "import", "path": path})


def create_shell_op(cmd: str) -> Op:
    return FrozenDict({"op_type": "shell", "cmd": cmd})


@dataclass
class ShellEnv:
    root_dir: Path
    outdir: Path = DEFAULT_OUTDIR
    storage: Storage = field(init=False)

    def __post_init__(self):
        if not self.root_dir.is_absolute():
            raise ValueError(f"expected absolute path but received '{self.root_dir}'")
        if not self.root_dir.exists():
            raise ValueError(f"{self.root_dir} does not exist")
        if not self.root_dir.is_dir():
            raise ValueError(f"{self.root_dir} is not a directory")
        self.storage = Storage(self.boyle_dir / _STORAGE_SUBDIR)
        self.outdir = self.root_dir / self.outdir

    @property
    def boyle_dir(self) -> Path:
        return self.root_dir / BOYLE_DIR

    def create_sandbox(self) -> SandboxKey:
        key = SandboxKey(uuid4().hex)
        path = self._get_sandbox_path(key)
        path.mkdir(parents=True)
        return key

    def destroy_sandbox(self, sandbox: SandboxKey):
        shutil.rmtree(self._get_sandbox_path(sandbox))

    def place(self, sandbox: SandboxKey, tree: Tree):
        self.storage.restore(tree, self._get_sandbox_path(sandbox))

    def stow(self, sandbox: SandboxKey, loc: Loc):
        path = self._get_sandbox_path(sandbox) / loc_to_rel_path(loc)
        return self.storage.store(path)

    def can_restore(self, tree: Tree) -> bool:
        return self.storage.can_restore(tree)

    def _get_sandbox_path(self, sandbox: SandboxKey) -> Path:
        return self.boyle_dir / "sandboxes" / sandbox

    def deliver(self, tree: Tree):
        if self.outdir.exists():
            shutil.rmtree(self.outdir)
        self.storage.restore(tree, self.outdir)

    def run_op(self, op: Op, sandbox: SandboxKey):
        if not isinstance(op, Mapping):
            raise ValueError(f"expected Mapping but received {op}")
        op_type = op["op_type"]
        if op_type == "shell":
            cmd = op["cmd"]
            if not isinstance(cmd, str):
                raise ValueError(f"expected str cmd but received {cmd}")
            work_dir = self._get_sandbox_path(sandbox)
            subprocess.run(cmd, shell=True, cwd=work_dir)
        elif op_type == "import":
            src_path = self.root_dir / cast(str, op["path"])
            if not src_path.exists():
                raise FileNotFoundError(src_path)

            tree_to_import = describe(src_path)

            if self.storage.can_restore(tree_to_import):
                tree_to_place = Tree.from_nested_items({IMPORT_LOC: tree_to_import})
                self.place(sandbox, tree_to_place)
            else:
                dst_path = self._get_sandbox_path(sandbox) / loc_to_rel_path(IMPORT_LOC)
                if src_path.is_file():
                    shutil.copy(src_path, dst_path)
                elif src_path.is_dir():
                    shutil.copytree(src_path, dst_path)
                else:
                    raise ValueError(f"what kind of thing is that at {src_path}?")


@dataclass(frozen=True)
class ShellSystem:
    root_dir: Path
    _env: ShellEnv = field(init=False)
    _node_runner: NodeRunner = field(init=False)

    def __post_init__(self):
        object.__setattr__(self, "_env", ShellEnv(self.root_dir))
        object.__setattr__(self, "_node_runner", NodeRunner(self._env))

    @property
    def outdir(self):
        return self._env.outdir

    def make(self, node: Node):
        results = boyleworkflow.scheduling.make({node}, self._node_runner)
        self._env.deliver(results[node])
