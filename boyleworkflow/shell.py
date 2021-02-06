from __future__ import annotations
import subprocess
import shutil
from uuid import uuid4
from boyleworkflow.loc import Loc
from dataclasses import dataclass, field
from pathlib import Path
from boyleworkflow.tree import Tree
from boyleworkflow.calc import Op, SandboxKey
from boyleworkflow.storage import Storage, loc_to_rel_path


BOYLE_DIR = ".boyle"
DEFAULT_OUTDIR = Path("output")
_STORAGE_SUBDIR = "storage"

@dataclass
class ShellEnv:
    root_dir: Path
    outdir: Path = DEFAULT_OUTDIR
    storage: Storage = field(init=False)

    def __post_init__(self):
        if not self.root_dir.is_absolute():
            raise ValueError(f"expected absolute path but received {self.root_dir}")
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
        self.storage.restore(tree, self.outdir)

    def run_op(self, op: Op, sandbox: SandboxKey):
        if not isinstance(op, str):
            raise ValueError(f"expected string Op but received {op}")
        work_dir = self._get_sandbox_path(sandbox)
        subprocess.run(op, shell=True, cwd=work_dir)
