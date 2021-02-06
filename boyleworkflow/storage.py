from __future__ import annotations
import os
from boyleworkflow.loc import Loc, Name
from dataclasses import dataclass
from pathlib import Path
import hashlib
from boyleworkflow.tree import Tree

HASH_NAME = "sha256"
HASH = getattr(hashlib, HASH_NAME)
_CHUNK_SIZE = 2 ** 20
_STORAGE_PATH_SPLIT_LEN = 2


def _digest_file(path: Path) -> str:
    m = HASH()

    with open(path, "rb") as f:
        while True:
            data = f.read(_CHUNK_SIZE)
            if not data:
                return m.hexdigest()
            m.update(data)


def _describe_file(path: Path) -> Tree:
    return Tree({}, {HASH_NAME: _digest_file(path)})


def _describe_dir(path: Path) -> Tree:
    return Tree({Name(child.name): describe(child) for child in path.iterdir()})


def describe(path: Path) -> Tree:
    if not path.exists():
        raise FileNotFoundError(path)
    if path.is_dir():
        return _describe_dir(path)
    elif path.is_file():
        return _describe_file(path)

    raise NotImplementedError()


def _represents_file(tree: Tree):
    return tree.data is not None


def loc_to_rel_path(loc: Loc):
    path = Path(str(loc))
    assert not path.is_absolute(), path
    return path


@dataclass
class Storage:
    root_path: Path

    def __post_init__(self):
        if self.root_path.exists():
            if not self.marker_file_path.exists():
                raise FileNotFoundError(f"expected to find {self.marker_file_path}")
        else:
            self.root_path.mkdir(parents=True)
            self.marker_file_path.touch()

    @property
    def marker_file_path(self):
        return self.root_path / ".boyle-storage"

    def store(self, path: Path) -> Tree:
        tree = describe(path)
        self._store_tree(tree, path)
        return tree

    def _store_tree(self, tree: Tree, start_path: Path):
        for loc, subtree in tree.walk():
            if _represents_file(subtree):
                self._store_file(subtree, start_path / loc_to_rel_path(loc))

    def _store_file(self, file_tree: Tree, src_path: Path):
        dst_path = self._get_storage_path(file_tree)
        dst_path.parent.mkdir(exist_ok=True, parents=True)
        os.link(src_path, dst_path)

    def restore(self, tree: Tree, path: Path):
        if _represents_file(tree):
            self._restore_file(tree, path)
        else:
            self._restore_dir(tree, path)

    def _restore_file(self, file_tree: Tree, dst_path: Path):
        src_path = self._get_storage_path(file_tree)
        os.link(src_path, dst_path)

    def _restore_dir(self, dir_tree: Tree, dst_path: Path):
        dst_path.mkdir(exist_ok=True)

        for name, subtree in dir_tree.items():
            child_path = dst_path / str(name)
            self.restore(subtree, child_path)

    def _get_storage_path(self, file_tree: Tree) -> Path:
        hexdigest: str = file_tree.data[HASH_NAME]  # type: ignore
        return (
            self.root_path
            / HASH_NAME
            / hexdigest[:_STORAGE_PATH_SPLIT_LEN]
            / hexdigest[_STORAGE_PATH_SPLIT_LEN:]
        )

    def can_restore(self, tree: Tree) -> bool:
        for _, subtree in tree.walk():
            if _represents_file(subtree):
                if not self._get_storage_path(subtree).exists():
                    return False

        return True
