from __future__ import annotations
from typing import Protocol
from boyleworkflow.trees import Tree, Indexed
from boyleworkflow.calcs import Calc, Run, Glob
from pathlib import Path


class NotFoundException(Exception):
    pass


class ConflictException(Exception):
    pass


class Log(Protocol):
    def search_result(self, calc: Calc, glob: Glob) -> Indexed[Tree]:
        """
        Get the Indexed[Tree] resulting from a Calc restricted to a Glob.

        Raises:
            NotFoundException if log has no result.
            ConflictException if log has multiple conflicting results.
        """
        ...

    def save_run(self, run: Run):
        """
        Save a Run with dependencies.

        Save:
            * The Op
            * The input Tree
            * The Run itself (including the result Trees))
        """
        ...


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