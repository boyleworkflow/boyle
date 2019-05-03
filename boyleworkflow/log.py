from typing import Optional, Mapping, Iterable
import os
import sqlite3
import logging
import pkg_resources
import datetime
import uuid
import attr

import boyleworkflow
from boyleworkflow.core import Op, Calc, Comp, PathLike, Loc, Digest, get_upstream_sorted

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 'v0.1.0'

sqlite3.register_adapter(datetime.datetime, lambda dt: dt.isoformat())


Opinion = Optional[bool]


class NotFoundException(Exception):
    pass


class ConflictException(Exception):
    pass


class Log:
    @staticmethod
    def create(path: PathLike):
        """
        Create a new Log database.

        Args:
            path (str): Where to create the database.
        """
        schema_path = pkg_resources.resource_filename(
            __name__, "resources/schema-{}.sql".format(SCHEMA_VERSION)
        )

        with open(schema_path, 'r') as f:
            schema_script = f.read()

        conn = sqlite3.connect(path)

        with conn:
            conn.executescript(schema_script)

        conn.close()

    def __init__(self, path: PathLike):
        if not os.path.exists(path):
            Log.create(path)
        self.conn = sqlite3.connect(path, isolation_level='IMMEDIATE')
        self.conn.execute('PRAGMA foreign_keys = ON;')

    def close(self):
        self.conn.close()

    def save_calc(self, calc: Calc):
        with self.conn:
            self.conn.execute(
                'INSERT OR IGNORE INTO op(op_id, definition) VALUES (?, ?)',
                (calc.op.op_id, calc.op.definition),
            )

            self.conn.execute(
                'INSERT OR IGNORE INTO calc(calc_id, op_id) VALUES (?, ?)',
                (calc.calc_id, calc.op.op_id),
            )

            self.conn.executemany(
                'INSERT OR IGNORE INTO calc_input (calc_id, loc, digest) '
                'VALUES (?, ?, ?)',
                [(calc.calc_id, loc, digest) for loc, digest in calc.inputs.items()],
            )

    def save_run(
        self,
        calc: Calc,
        results: Mapping[Loc, Digest],
        start_time: datetime.datetime,
        end_time: datetime.datetime,
    ):
        run_id = str(uuid.uuid4())

        self.save_calc(calc)

        with self.conn:
            self.conn.execute(
                'INSERT INTO run '
                '(run_id, calc_id, start_time, end_time) '
                'VALUES (?, ?, ?, ?)',
                (run_id, calc.calc_id, start_time, end_time),
            )

            self.conn.executemany(
                'INSERT INTO result (run_id, loc, digest) VALUES (?, ?, ?)',
                [(run_id, loc, digest) for loc, digest in results.items()],
            )


    def save_comp(self, leaf_comp: Comp):
        with self.conn:
            for comp in get_upstream_sorted([leaf_comp]):
                self.conn.execute(
                    'INSERT OR IGNORE INTO comp (comp_id, op_id, loc) '
                    'VALUES (?, ?, ?)',
                    (comp.comp_id, comp.op.op_id, comp.loc),
                )

                self.conn.executemany(
                    'INSERT OR IGNORE INTO comp_input '
                    '(comp_id, loc, input_comp_id) '
                    'VALUES (?, ?, ?)',
                    [
                        (comp.comp_id, loc, inp_comp.comp_id)
                        for loc, inp_comp in comp.inputs.items()
                    ],
                )

    def save_response(
        self, comp: Comp, digest: Digest, time: datetime.datetime
    ):
        self.save_comp(comp)

        with self.conn:
            self.conn.execute(
                'INSERT OR IGNORE INTO response '
                '(comp_id, digest, first_time) '
                'VALUES (?, ?, ?)',
                (comp.comp_id, digest, time),
            )

    def set_trust(self, calc_id: str, loc: Loc, digest: Digest, opinion: bool):
        with self.conn:
            self.conn.execute(
                'INSERT OR REPLACE INTO trust '
                '(calc_id, loc, digest, opinion) '
                'VALUES (?, ?, ?, ?) ',
                (calc_id, loc, digest, opinion),
            )

    def get_opinions(self, calc: Calc, loc: Loc) -> Mapping[Digest, Opinion]:
        query = self.conn.execute(
            'SELECT digest, opinion FROM result '
            'INNER JOIN run USING (run_id) '
            'LEFT OUTER JOIN trust USING (calc_id, loc, digest) '
            'WHERE (loc = ? AND calc_id = ?)',
            (loc, calc.calc_id),
        )

        return {digest: opinion for digest, opinion in query}

    def get_result(self, calc: Calc, loc: Loc) -> Digest:
        opinions = self.get_opinions(calc, loc)

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
        elif len(candidates) == 1:
            digest, = candidates
            return digest
        else:
            raise ConflictException(opinions)

    def get_calc(self, comp: Comp) -> Calc:
        def get_comp_result(input_comp: Comp) -> Digest:
            calc = self.get_calc(input_comp)
            return self.get_result(calc, input_comp.loc)

        return Calc(
            inputs={
                loc: get_comp_result(inp_comp)
                for loc, inp_comp in comp.inputs.items()
            },
            op=comp.op,
        )
