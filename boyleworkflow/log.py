from typing import NewType, Optional, Iterable, Generator
import sqlite3
import logging
import datetime
import sys
from pathlib import Path

assert sys.version_info.major == 3

import importlib.resources as importlib_resources

from boyleworkflow.trees import Tree, Indexed, TreeId
from boyleworkflow.calcs import Calc, Glob, Run, Op
from boyleworkflow.util import digest_str, unique_json, unique_json_digest

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "v0.3.0"
SCHEMA_PATH = f"schema-{SCHEMA_VERSION}.sql"

sqlite3.register_adapter(datetime.datetime, lambda dt: dt.isoformat())

Opinion = Optional[bool]


class NotFoundException(Exception):
    pass


class ConflictException(Exception):
    pass


class Log:
    @staticmethod
    def create(path: Path):
        """
        Create a new Log database.

        Args:
            path: Where to create the database.
        """
        with importlib_resources.path(
            "boyleworkflow.resources", SCHEMA_PATH
        ) as schema_path:
            with open(schema_path, "r") as f:
                schema_script = f.read()

        conn = sqlite3.connect(str(path))

        with conn:
            conn.executescript(schema_script)

        conn.close()

    def __init__(self, path: Path):
        if not path.exists():
            Log.create(path)
        self.conn = sqlite3.connect(str(path), isolation_level="IMMEDIATE")
        self.conn.execute("PRAGMA foreign_keys = ON;")

    def close(self):
        self.conn.close()

    def search_result(self, calc: Calc, glob: Glob) -> Indexed[Tree]:
        """
        Get the Indexed[Tree] resulting from a Calc restricted to a Glob.

        Raises:
            NotFoundException if log has no result.
            ConflictException if log has multiple conflicting results.
        """
        tree_ids = self._get_not_distrusted_tree_ids(calc, glob)

        unique_tree_ids = set(tree_ids)

        # If there is no tree, nothing is found.
        if len(unique_tree_ids) == 0:
            raise NotFoundException(calc, glob)
        if len(unique_tree_ids) > 1:
            raise ConflictException(calc, glob)

        (tree_id,) = unique_tree_ids

        return self._get_tree(tree_id)

    def _get_not_distrusted_tree_ids(
        self, calc: Calc, glob: Glob
    ) -> Iterable[TreeId]:
        ...

    def _get_tree(self, tree_id: TreeId) -> Indexed[Tree]:
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

    def _save_op(self, op: Op):
        op_unique_json = unique_json(op)
        op_id = digest_str(op_unique_json)
        self.conn.execute(
            "INSERT OR IGNORE INTO op(op_id, definition) VALUES (?, ?)",
            (op_id, op_unique_json),
        )

    def _save_tree(self, tree: Tree):
        ...

    def save_run(self, run: Run):
        """
        Save a Run with dependencies.

        Save:
            * The Op
            * The Run itself (including the result Tree)
        """

        with self.conn:
            self._save_op(run.calc.op)
            self._save_tree(run.output_tree)

            self.conn.execute(
                "INSERT INTO run "
                "(run_id, op_id, inp_digest, out_digest, start_time, end_time) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    run.run_id,
                    run.calc.op.op_id,
                    run.calc.input_tree.tree_id,
                    run.output_tree.tree_id,
                    run.start_time,
                    run.end_time,
                ),
            )

    def _save_node_and_inputs_and_outputs(self, node: Node):
        self.conn.execute(
            "INSERT OR IGNORE INTO node "
            "(node_id, op_id, depth) "
            "VALUES (?, ?, ?)",
            (node.node_id, node.op.op_id, node.depth),
        )

        self.conn.executemany(
            "INSERT OR IGNORE INTO node_input "
            "(node_id, loc, parent_node_id, parent_loc) "
            "VALUES (?, ?, ?, ?)",
            [
                (node.node_id, loc, defn.node.node_id, defn.loc)
                for loc, defn in node.inputs.items()
            ],
        )

        self.conn.executemany(
            "INSERT OR IGNORE INTO defn (node_id, loc) VALUES (?, ?)",
            [(node.node_id, loc) for loc in node.outputs],
        )

    def save_node_result(self, node_result: NodeResult):
        """
        Save a NodeResult with dependencies.

        This notes the Node as (one possible) provenance of a Tree.

        Save:
            * The Node and connections to parent Nodes (but not upstream nodes)
            * The NodeResult itself
        """

        with self.conn:
            self._save_node_and_inputs_and_outputs(node_result.node)

            self.conn.execute(
                "INSERT OR IGNORE INTO node_result "
                "(node_id, output_tree_id, explicit, first_time) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    node_result.node.node_id,
                    node_result.output_tree.tree_id,
                    node_result.explicit,
                    node_result.time,  # First time, if inserting
                ),
            )

    def get_calc(self, node: Node) -> Calc:
        input_resources = {
            loc: self.get_node_result(parent_defn.node)[parent_defn.loc]
            for loc, parent_defn in node.inputs.items()
        }

        input_tree = Tree.from_resources(input_resources)

        return Calc(node.op, input_tree)

    def get_node_result(self, node: Node) -> Digest:
        calc = self.get_calc(node)
        return self.get_calc_result(calc, node.outputs)

    # def set_trust(self, calc_id: str, loc: Loc, digest: Digest, opinion: bool):
    #     with self.conn:
    #         self.conn.execute(
    #             "INSERT OR REPLACE INTO trust "
    #             "(calc_id, loc, digest, opinion) "
    #             "VALUES (?, ?, ?, ?) ",
    #             (calc_id, loc, digest, opinion),
    #         )
