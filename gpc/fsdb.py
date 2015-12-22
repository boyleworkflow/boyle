import os
import json
import sqlite3
import functools

from gpc import common

from gpc.common import unique_json, hexdigest

def save_stmt(data_dir, stmt):
    if stmt.lstrip().lower().startswith('insert'):
        digest = hexdigest(stmt)
        path = os.path.join(data_dir, digest)
        if not os.path.exists(path):
            with open(path, 'w') as file:
                file.write(stmt)
                file.write('\n')


class Database(object):
    """Class to interact with the file-backed SQLlite database."""
    def __init__(self, path):
        super(Database, self).__init__()
        self._path = os.path.abspath(path)
        self._data_path = os.path.join(self._path, 'data')
        self._schema_path = os.path.join(self._path, 'schema.sql')


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

        saver = functools.partial(save_stmt, db._data_path)
        db._conn.set_trace_callback(saver)
        return db


    @classmethod
    def create(cls, path, schema):
        db = cls(path)
        os.makedirs(db._path)
        os.makedirs(db._data_path)
        open(db._schema_path, 'w').write(schema)

        
    def execute(self, sql, parameters=()):
        with self._conn:
            result = self._conn.execute(sql, parameters)
            return result
