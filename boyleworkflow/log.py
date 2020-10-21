from typing import Optional, Mapping, Iterable, Generator
import os
import sqlite3
import logging
import datetime
import uuid
import attr
import sys

assert sys.version_info.major == 3

if sys.version_info.minor >= 7:
    import importlib.resources as importlib_resources
else:
    import importlib_resources

import boyleworkflow
from boyleworkflow.core import (
    Op,
    Calc,
    Node,
    Defn,
    NodeResult,
    Loc,
    Tree,
    Run,
    TreeConflictException,
    NotFoundException,
    ConflictException,
)
from boyleworkflow.storage import Digest

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "v0.3.0"
SCHEMA_PATH = f"schema-{SCHEMA_VERSION}.sql"

sqlite3.register_adapter(datetime.datetime, lambda dt: dt.isoformat())


Opinion = Optional[bool]


class Log(boyleworkflow.core.Log):
    @staticmethod
    def create(path: os.PathLike):
        """
        Create a new Log database.

        Args:
            path (str): Where to create the database.
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

    def __init__(self, path: os.PathLike):
        if not os.path.exists(path):
            Log.create(path)
        self.conn = sqlite3.connect(str(path), isolation_level="IMMEDIATE")
        self.conn.execute("PRAGMA foreign_keys = ON;")

    def close(self):
        self.conn.close()

    def _save_op(self, op: Op):
        self.conn.execute(
            "INSERT OR IGNORE INTO op(op_id, definition) VALUES (?, ?)",
            (op.op_id, op.definition),
        )

    def _save_tree(self, tree: Tree):
        raise NotImplementedError()

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

    def get_calc_result(self, calc: Calc, outputs: Iterable[Loc]) -> Tree:
        trees = self._generate_trusted_trees(calc, outputs)

        # If there is no tree, nothing is found.
        # If there is exactly one tree, it can be used.
        # If there are more than one, check that they agree.

        try:
            # If the first raises StopIteration there is no (trusted) result.
            tree = next(trees)
        except StopIteration:
            raise NotFoundException(calc)

        try:
            for other in trees:
                tree = tree.merge(other)
        except TreeConflictException as e:
            raise ConflictException(calc) from e

        return tree

    def get_node_result(self, node: Node) -> Digest:
        calc = self.get_calc(node)
        return self.get_calc_result(calc, node.outputs)

    def _generate_trusted_trees(
        self, calc: Calc, outputs: Iterable[Loc]
    ) -> Generator[Tree, None, None]:
        # query = self.conn.execute(
        #     "SELECT digest, opinion FROM result "
        #     "INNER JOIN run USING (run_id) "
        #     "LEFT OUTER JOIN trust USING (calc_id, loc, digest) "
        #     "WHERE (loc = ? AND calc_id = ?)",
        #     (loc, calc.calc_id),
        # )

        # return {digest: opinion for digest, opinion in query}
        raise NotImplementedError()

    # def set_trust(self, calc_id: str, loc: Loc, digest: Digest, opinion: bool):
    #     with self.conn:
    #         self.conn.execute(
    #             "INSERT OR REPLACE INTO trust "
    #             "(calc_id, loc, digest, opinion) "
    #             "VALUES (?, ?, ?, ?) ",
    #             (calc_id, loc, digest, opinion),
    #         )
