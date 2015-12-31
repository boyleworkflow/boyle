import os
import json
import sqlite3
import functools

from gpc import common

from gpc.common import unique_json, hexdigest



class Database(object):
    """Class to interact with the file-backed SQLlite database."""
    def __init__(self, path):
        super(Database, self).__init__()
        self._path = os.path.abspath(path)
        self._data_path = os.path.join(self._path, 'data')
        self._schema_path = os.path.join(self._path, 'schema.sql')
        self._transaction = []

    def _handle_stmt(self, stmt):
        if stmt.strip() in ('BEGIN', 'ROLLBACK'):
            self._transaction = []
        elif stmt == 'COMMIT':
            for s in self._transaction:
                self._write_statement(s)
        else:
            self._transaction.append(stmt)

    def _write_statement(self, stmt):
        if stmt.lstrip().lower().startswith('insert'):
            digest = hexdigest(stmt)
            path = os.path.join(self._data_path, digest)
            if not os.path.exists(path):
                with open(path, 'w') as file:
                    file.write(stmt)
                    file.write('\n')


    @classmethod
    def load(cls, path):
        db = cls(path)

        db._conn = sqlite3.connect(':memory:')
        schema = open(db._schema_path, 'r').read()

        db._conn.executescript(schema)

        with db._conn as conn:
            for filename in os.listdir(db._data_path):
                path = os.path.join(db._data_path, filename)
                sql = open(path, 'r').read()
                conn.execute(sql)

        db._conn.set_trace_callback(db._handle_stmt)
        return db


    @classmethod
    def create(cls, path, schema):
        db = cls(path)
        os.makedirs(db._path)
        os.makedirs(db._data_path)
        open(db._schema_path, 'w').write(schema)


    def __enter__(self, *args, **kwargs):
        self._conn.__enter__(*args, **kwargs)
        return self

    def __exit__(self, *args, **kwargs):
        self._conn.__exit__(*args, **kwargs)
        
    def execute(self, sql, parameters=()):
        return self._conn.execute(sql, parameters)
