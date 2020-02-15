from typing import Optional, Mapping, Iterable
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
    DefnResult,
    Loc,
    Run,
    NodeId,
    NotFoundException,
    ConflictException,
    Index,
    IndexKey,
    SINGLE_KEY,
)
from boyleworkflow.storage import Digest

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "v0.2.0"
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

    def _save_calc_and_inputs(self, calc: Calc):
        self.conn.execute(
            "INSERT OR IGNORE INTO calc(calc_id, op_id) VALUES (?, ?)",
            (calc.calc_id, calc.op.op_id),
        )

        self.conn.executemany(
            "INSERT OR IGNORE INTO input (calc_id, loc, digest) "
            "VALUES (?, ?, ?)",
            [
                (calc.calc_id, loc, digest)
                for loc, digest in calc.inputs.items()
            ],
        )

    def _save_calc_results(self, run: Run):
        self.conn.executemany(
            "INSERT INTO calc_result (run_id, loc, digest) VALUES (?, ?, ?)",
            [(run.run_id, loc, digest) for loc, digest in run.results.items()],
        )

    def save_run(self, run: Run):
        """
        Save a Run with dependencies.

        Save:
            * The Op
            * The Calc (including its inputs)
            * The Run (including the results)
        """

        with self.conn:
            self._save_op(run.calc.op)
            self._save_calc_and_inputs(run.calc)

            self.conn.execute(
                "INSERT INTO run "
                "(run_id, calc_id, start_time, end_time) "
                "VALUES (?, ?, ?, ?)",
                (run.run_id, run.calc.calc_id, run.start_time, run.end_time),
            )

            self._save_calc_results(run)

    def _save_node_and_inputs_and_outputs(self, node: Node):
        self.conn.execute(
            "INSERT OR IGNORE INTO node "
            "(node_id, op_id, index_node_id) "
            "VALUES (?, ?, ?)",
            (node.node_id, node.op.op_id, node.index_defn.node.node_id),
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

    def save_node(self, node: Node):
        """
        Save a Node.

        Save:
            * The Node itself
            * The Op
            * Connections to upstream nodes
            * Defns
        """
        with self.conn:
            self._save_op(node.op)
            self._save_node_and_inputs_and_outputs(node)

    @staticmethod
    def _get_iloc(index: Index, key: IndexKey):
        for i, potential_match in enumerate(index):
            if key == potential_match:
                return i

        raise ValueError(f"could not find key {key}")


    def save_defn_result(self, defn_result: DefnResult):
        iloc = self._get_iloc(defn_result.index, defn_result.key)

        with self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO defn_result "
                "(node_id, loc, index_digest, iloc, explicit, result_digest, "
                "first_time) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    defn_result.defn.node.node_id,
                    defn_result.defn.loc,
                    defn_result.index.digest,
                    iloc,
                    defn_result.explicit,
                    defn_result.digest,
                    defn_result.time,
                ),
            )

    def save_index(self, index: Index):
        with self.conn:
            self.conn.execute(
                "INSERT OR IGNORE INTO index_result (digest, data) "
                "VALUES (?, ?)",
                (index.digest, index.data),
            )

    def get_index(self, node: Node) -> Index:
        index_digest = self.get_defn_result(node.index_defn, SINGLE_KEY)
        query = self.conn.execute(
            "SELECT data FROM index_result WHERE (digest = ?)", (index_digest,)
        )
        data, = query.fetchone()
        return Index(data)

    def get_calc_result(self, calc: Calc, loc: Loc) -> Digest:
        opinions = self._get_opinions(calc, loc)

        candidates = [
            digest
            for digest, opinion in opinions.items()
            if not opinion == False
        ]

        # If there is no digest left, nothing is found.
        # If there is exactly one left, it can be used.
        # If there is more than one left, there is a conflict.

        if not candidates:
            raise NotFoundException((calc, loc))
        elif len(candidates) > 1:
            raise ConflictException(opinions)

        digest, = candidates

        return digest

    def get_calc(self, node: Node, key: IndexKey) -> Calc:
        return Calc(
            node.op,
            {
                loc: self.get_defn_result(defn, key)
                for loc, defn in node.inputs.items()
            },
        )

    def get_defn_result(self, defn: Defn, key: IndexKey) -> Digest:
        calc = self.get_calc(defn.node, key)
        return self.get_calc_result(calc, defn.loc)

    # def set_trust(self, calc_id: str, loc: Loc, digest: Digest, opinion: bool):
    #     with self.conn:
    #         self.conn.execute(
    #             "INSERT OR REPLACE INTO trust "
    #             "(calc_id, loc, digest, opinion) "
    #             "VALUES (?, ?, ?, ?) ",
    #             (calc_id, loc, digest, opinion),
    #         )

    def _get_opinions(self, calc: Calc, loc: Loc) -> Mapping[Digest, Opinion]:
        query = self.conn.execute(
            "SELECT digest, opinion FROM result "
            "INNER JOIN run USING (run_id) "
            "LEFT OUTER JOIN trust USING (calc_id, loc, digest) "
            "WHERE (loc = ? AND calc_id = ?)",
            (loc, calc.calc_id),
        )

        return {digest: opinion for digest, opinion in query}
