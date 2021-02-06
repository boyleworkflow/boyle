from __future__ import annotations
from boyleworkflow.util import freeze, unfreeze
import json
from uuid import uuid4
import datetime
import sqlite3
import importlib.resources as importlib_resources
from pathlib import Path
from boyleworkflow.frozendict import FrozenDict
from typing import List, Mapping, NewType, Optional, Protocol
from dataclasses import dataclass, field
from boyleworkflow.loc import Name, Loc
from boyleworkflow.tree import Tree, TreeData
from boyleworkflow.calc import Calc, CalcOut
import boyleworkflow.resources

sqlite3.register_adapter(datetime.datetime, lambda dt: dt.isoformat())

SCHEMA_VERSION = "v0.2.0"
_SCHEMA_FILENAME = f"schema-{SCHEMA_VERSION}.sql"
_SQLITE_IN_MEMORY_PATH = ":memory:"

class NotFound(Exception):
    pass


class ConflictingResults(Exception):
    pass


def create_uuid_hex_str() -> str:
    return uuid4().hex


RunId = NewType("RunId", str)


def generate_run_id() -> RunId:
    return RunId(create_uuid_hex_str())


@dataclass(frozen=True)
class Run:
    run_id: str = field(init=False, default_factory=create_uuid_hex_str)
    calc: Calc
    results: FrozenDict[Loc, Tree]


class CacheLog(Protocol):
    def save_run(self, run: Run):
        ...

    def recall_result(self, calc_out: CalcOut) -> Tree:
        ...


class Log:
    def __init__(self, path: Optional[Path] = None):
        path_str = str(path) if path else _SQLITE_IN_MEMORY_PATH
        self.conn = sqlite3.connect(path_str)
        self.conn.execute("PRAGMA foreign_keys = ON;")

        needs_initialize = not (path and path.exists())
        if needs_initialize:
            self._initialize()

    def _initialize(self):
        schema_script = importlib_resources.read_text(
            boyleworkflow.resources, _SCHEMA_FILENAME
        )

        with self.conn:
            self.conn.executescript(schema_script)

    def close(self):
        self.conn.close()

    def save_run(self, run: Run):
        if set(run.results) != set(run.calc.out):
            raise ValueError(f"mismatch {set(run.results)} and {set(run.calc.out)}")

        with self.conn:
            self._i_write_run(run)

    def recall_result(self, calc_out: CalcOut):
        tree_ids = self._i_read_result_tree_ids(calc_out)
        if not tree_ids:
            raise NotFound(calc_out)
        if len(tree_ids) > 1:
            raise ConflictingResults(calc_out, tree_ids)

        tree_id, = tree_ids

        return self._i_read_tree_by_id(tree_id)

    def _i_read_result_tree_ids(self, calc_out: CalcOut) -> List[str]:
        cur = self.conn.execute(
            "SELECT tree_id FROM run_result WHERE calc_out_id = ?",
            (calc_out.calc_out_id,),
        )
        results = list(cur)
        return [item[0] for item in results]

    def _i_write_run(self, run: Run):
        self.conn.execute(
            "INSERT INTO run(run_id) VALUES(?)",
            (run.run_id,),
        )
        self._i_write_run_results(run)
        for tree in run.results.values():
            self._i_write_tree(tree)

    def _i_write_run_results(self, run: Run):
        results_by_calc_out = {
            CalcOut(run.calc.inp, run.calc.op, out_loc): tree
            for out_loc, tree in run.results.items()
        }
        self.conn.executemany(
            "INSERT OR IGNORE INTO run_result (run_id, calc_out_id, tree_id) "
            "VALUES (?, ?, ?)",
            [
                (run.run_id, calc_out.calc_out_id, tree.tree_id)
                for calc_out, tree in results_by_calc_out.items()
            ],
        )

    def _i_write_tree(self, tree: Tree):
        self.conn.execute(
            "INSERT OR IGNORE INTO tree (tree_id, data_) VALUES (?, ?)",
            (tree.tree_id, json.dumps(unfreeze(tree.data))),
        )
        if not len(tree):
            return
        for subtree in tree.values():
            self._i_write_tree(subtree)

        self.conn.executemany(
            "INSERT OR IGNORE INTO tree_child (parent_tree_id, name_, tree_id) "
            "VALUES (?, ?, ?)",
            [
                (tree.tree_id, str(name), subtree.tree_id)
                for name, subtree in tree.items()
            ],
        )


    def _i_read_tree_by_id(self, tree_id: str) -> Tree:
        data = self._i_read_tree_data(tree_id)
        child_ids = self._i_read_tree_children(tree_id)
        children = {
            name: self._i_read_tree_by_id(child_tree_id)
            for name, child_tree_id in child_ids.items()
        }
        return Tree(children, data)

    def _i_read_tree_data(self, tree_id: str) -> TreeData:
        cur = self.conn.execute("SELECT data_ FROM tree WHERE tree_id = ?", (tree_id,))
        json_str = next(cur)[0]
        return freeze(json.loads(json_str))

    def _i_read_tree_children(self, tree_id: str) -> Mapping[Name, str]:
        cur = self.conn.execute(
            "SELECT name_, tree_id FROM tree_child WHERE parent_tree_id = ?",
            (tree_id,),
        )
        return {Name(name): str(child_tree_id) for name, child_tree_id in cur}

    # def save_calc(self, calc: Calc):
    #     with self.conn:
    #         self.conn.execute(
    #             "INSERT OR IGNORE INTO op(op_id, definition) VALUES (?, ?)",
    #             (calc.op.op_id, calc.op.definition),
    #         )

    #         self.conn.execute(
    #             "INSERT OR IGNORE INTO calc(calc_id, op_id) VALUES (?, ?)",
    #             (calc.calc_id, calc.op.op_id),
    #         )

    #         self.conn.executemany(
    #             "INSERT OR IGNORE INTO input (calc_id, loc, digest) "
    #             "VALUES (?, ?, ?)",
    #             [(calc.calc_id, inp.loc, inp.digest) for inp in calc.inputs],
    #         )

    # def save_run(
    #     self,
    #     calc: Calc,
    #     results: Iterable[Result],
    #     start_time: datetime.datetime,
    #     end_time: datetime.datetime,
    # ):
    #     run_id = str(uuid.uuid4())

    #     self.save_calc(calc)

    #     with self.conn:
    #         self.conn.execute(
    #             "INSERT INTO run "
    #             "(run_id, calc_id, start_time, end_time) "
    #             "VALUES (?, ?, ?, ?)",
    #             (run_id, calc.calc_id, start_time, end_time),
    #         )

    #         self.conn.executemany(
    #             "INSERT INTO result (run_id, loc, digest) VALUES (?, ?, ?)",
    #             [(run_id, result.loc, result.digest) for result in results],
    #         )

    # def save_comp(self, leaf_comp: Comp):
    #     with self.conn:
    #         for comp in get_upstream_sorted([leaf_comp]):
    #             self.conn.execute(
    #                 "INSERT OR IGNORE INTO comp (comp_id, op_id, loc) "
    #                 "VALUES (?, ?, ?)",
    #                 (comp.comp_id, comp.op.op_id, comp.loc),
    #             )

    #             self.conn.executemany(
    #                 "INSERT OR IGNORE INTO parent "
    #                 "(comp_id, parent_comp_id) "
    #                 "VALUES (?, ?)",
    #                 [(comp.comp_id, parent.comp_id) for parent in comp.parents],
    #             )

    # def save_response(self, comp: Comp, digest: Digest, time: datetime.datetime):
    #     self.save_comp(comp)

    #     with self.conn:
    #         self.conn.execute(
    #             "INSERT OR IGNORE INTO response "
    #             "(comp_id, digest, first_time) "
    #             "VALUES (?, ?, ?)",
    #             (comp.comp_id, digest, time),
    #         )

    # def set_trust(self, calc_id: str, loc: Loc, digest: Digest, opinion: bool):
    #     with self.conn:
    #         self.conn.execute(
    #             "INSERT OR REPLACE INTO trust "
    #             "(calc_id, loc, digest, opinion) "
    #             "VALUES (?, ?, ?, ?) ",
    #             (calc_id, loc, digest, opinion),
    #         )

    # def get_opinions(self, calc: Calc, loc: Loc) -> Mapping[Digest, Opinion]:
    #     query = self.conn.execute(
    #         "SELECT digest, opinion FROM result "
    #         "INNER JOIN run USING (run_id) "
    #         "LEFT OUTER JOIN trust USING (calc_id, loc, digest) "
    #         "WHERE (loc = ? AND calc_id = ?)",
    #         (loc, calc.calc_id),
    #     )

    #     return {digest: opinion for digest, opinion in query}

    # def get_result(self, calc: Calc, loc: Loc) -> Result:
    #     opinions = self.get_opinions(calc, loc)

    #     candidates = [
    #         digest for digest, opinion in opinions.items() if not opinion == False
    #     ]

    #     # If there is no digest left, nothing is found.
    #     # If there is exactly one left, it can be used.
    #     # If there is more than one left, there is a conflict.

    #     if not candidates:
    #         raise NotFoundException((calc, loc))
    #     elif len(candidates) == 1:
    #         (digest,) = candidates
    #         return Result(loc, digest)
    #     else:
    #         raise ConflictException(opinions)

    # def get_calc(self, comp: Comp) -> Calc:
    #     def get_comp_result(input_comp: Comp) -> Result:
    #         calc = self.get_calc(input_comp)
    #         return self.get_result(calc, input_comp.loc)

    #     return Calc(
    #         inputs=tuple(get_comp_result(parent) for parent in comp.parents),
    #         op=comp.op,
    #     )
