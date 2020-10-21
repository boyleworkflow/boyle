from __future__ import annotations
from typing import Protocol
from boyleworkflow.trees import Tree
from boyleworkflow.calcs import Glob
from pathlib import Path



class Storage(Protocol):
    def can_restore(self, tree: Tree) -> bool:
        """
        Does the storage have data to restore a given Tree?
        """
        ...

    def restore(self, tree: Tree, path: Path):
        """
        Restore a Tree to the given Path.

        Will not overwrite anything.

        Raises:
            NotADirectoryError if the path is not a directory.
            FileExistsError if anything would be overwritten.
        """
        ...

    def store(self, path: Path, glob: Glob) -> Tree:
        """
        Store what is matched by a Glob from a Path, and return a Tree.
        """
        ...


class Context(Protocol):
    log: Log
    storage: Storage